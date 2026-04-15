"""CPU/Memory monitoring and Feishu notification."""

import logging
import subprocess
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional

import lark_oapi as lark

from src.config import Config
from src.utils import create_feishu_client, send_feishu_message

logger = logging.getLogger(__name__)


@dataclass
class CPUInfo:
    """Represent CPU/Memory information."""

    hostname: str
    cpu_usage: float
    memory_used: float
    memory_total: float
    memory_percent: float
    load_average: List[float]

    def __init__(
        self,
        hostname: str,
        cpu_usage: float,
        memory_used: float,
        memory_total: float,
        memory_percent: float,
        load_average: Optional[List[float]] = None,
    ):
        """
        Initialize CPU/Memory info.

        Args:
            hostname: Host name where monitoring is performed.
            cpu_usage: CPU usage percentage (0-100).
            memory_used: Used memory in MB or GB.
            memory_total: Total memory in MB or GB.
            memory_percent: Memory usage percentage (0-100).
            load_average: Load average (1min, 5min, 15min).
        """
        self.hostname = hostname
        self.cpu_usage = cpu_usage
        self.memory_used = memory_used
        self.memory_total = memory_total
        self.memory_percent = memory_percent
        self.load_average = load_average or []

    def __str__(self) -> str:
        """Return formatted string representation."""
        la_str = ""
        if self.load_average:
            la_str = f" LA: {self.load_average[0]:.2f}/{self.load_average[1]:.2f}/{self.load_average[2]:.2f}"

        return (
            f"CPU: {self.cpu_usage:.1f}% | "
            f"Mem: {self.memory_used:.1f}/{self.memory_total:.1f}GB ({self.memory_percent:.1f}%){la_str}"
        )


class CPUMonitor:
    """Monitor CPU/Memory status on remote servers."""

    def __init__(self, hostnames: List[str], timeout: int = 30):
        """
        Initialize CPU monitor.

        Args:
            hostnames: List of host names from SSH config.
            timeout: SSH connection timeout in seconds.
        """
        self.hostnames = hostnames
        self.timeout = timeout

    def _execute_remote_command(self, hostname: str) -> Optional[str]:
        """
        Execute CPU/Memory monitoring command on remote host.

        Args:
            hostname: Host name from SSH config (SSH will auto-resolve).

        Returns:
            Command output as string, or None if failed.
        """
        try:
            # Use shell commands to get CPU and memory info
            # Get CPU usage from top (batch mode, 1 iteration)
            # Get memory info from free command
            remote_command = "top -bn1 | grep 'Cpu(s)' && free -g && uptime"

            cmd = [
                "ssh",
                "-o", "StrictHostKeyChecking=no",
                "-o", f"ConnectTimeout={self.timeout}",
                hostname,
                remote_command
            ]

            # Execute command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout + 10,
            )

            if result.returncode != 0:
                logger.error(f"Command failed on {hostname}: {result.stderr}")
                return None

            return result.stdout

        except subprocess.TimeoutExpired:
            logger.error(f"SSH connection timeout to {hostname}")
            return None
        except Exception as e:
            logger.error(f"Error executing command on {hostname}: {e}")
            return None

    def _parse_command_output(self, output: str, hostname: str) -> Optional[CPUInfo]:
        """
        Parse command output.

        Args:
            output: Command output string.
            hostname: Host name where monitoring was performed.

        Returns:
            CPUInfo object or None if parsing failed.
        """
        try:
            lines = output.strip().split('\n')

            cpu_usage = 0.0
            mem_used = 0.0
            mem_total = 0.0
            mem_percent = 0.0
            load_average = []

            for line in lines:
                # Parse CPU usage from top output
                # Format: %Cpu(s):  5.2 us,  2.1 sy,  0.0 ni, 92.0 id,  0.0 wa,  0.0 hi,  0.7 si,  0.0 st
                if 'Cpu(s)' in line or '%Cpu' in line:
                    # Extract idle percentage and calculate usage
                    match = re.search(r'(\d+\.?\d*)\s*id', line)
                    if match:
                        idle = float(match.group(1))
                        cpu_usage = 100.0 - idle

                # Parse memory from free -g output
                # Format: Mem:   64576    12345    52231     ...
                elif line.startswith('Mem:'):
                    parts = line.split()
                    if len(parts) >= 3:
                        mem_total = float(parts[1])
                        mem_used = float(parts[2])
                        if mem_total > 0:
                            mem_percent = (mem_used / mem_total) * 100

                # Parse load average from uptime
                # Format: load average: 1.50, 1.23, 0.98
                elif 'load average' in line:
                    match = re.search(r'load average:\s*([\d.]+),\s*([\d.]+),\s*([\d.]+)', line)
                    if match:
                        load_average = [
                            float(match.group(1)),
                            float(match.group(2)),
                            float(match.group(3))
                        ]

            return CPUInfo(
                hostname=hostname,
                cpu_usage=cpu_usage,
                memory_used=mem_used,
                memory_total=mem_total,
                memory_percent=mem_percent,
                load_average=load_average,
            )

        except Exception as e:
            logger.error(f"Error parsing output from {hostname}: {e}")
            return None

    def monitor_host(self, hostname: str) -> Optional[CPUInfo]:
        """
        Monitor CPU/Memory on a specific host.

        Args:
            hostname: Host name from SSH config.

        Returns:
            CPUInfo object or None if failed.
        """
        logger.info(f"Monitoring CPU/Memory on {hostname}...")
        output = self._execute_remote_command(hostname)

        if not output:
            logger.warning(f"No data received from {hostname}")
            return None

        cpu_info = self._parse_command_output(output, hostname)

        if cpu_info:
            logger.info(f"Got CPU/Memory data from {hostname}: CPU {cpu_info.cpu_usage:.1f}%, Mem {cpu_info.memory_percent:.1f}%")
        else:
            logger.warning(f"No valid CPU/Memory data parsed from {hostname}")

        return cpu_info

    def monitor_all(self, max_workers: int = 5) -> Dict[str, CPUInfo]:
        """
        Monitor CPU/Memory on all hosts concurrently.

        Args:
            max_workers: Maximum number of concurrent SSH connections.

        Returns:
            Dictionary mapping host names to CPUInfo objects.
        """
        results = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_host = {
                executor.submit(self.monitor_host, hostname): hostname
                for hostname in self.hostnames
            }

            for future in as_completed(future_to_host):
                hostname = future_to_host[future]
                try:
                    cpu_info = future.result()
                    if cpu_info:
                        results[hostname] = cpu_info
                except Exception as e:
                    logger.error(f"Error monitoring {hostname}: {e}")

        return results


class FeishuCPUMonitor:
    """CPU/Memory monitoring with Feishu notifications."""

    def __init__(self, config: Config):
        """
        Initialize Feishu CPU monitor.

        Args:
            config: Configuration object.
        """
        self.config = config
        self.client = self._create_client()

        # Get CPU node names from config
        self.hostnames = config.cpu_node_names
        if not self.hostnames:
            raise ValueError("No CPU node names configured in config.yaml")

        self.cpu_threshold = config.cpu_threshold * 100  # Convert to percentage
        self.memory_threshold = config.memory_threshold * 100  # Convert to percentage

        logger.info(f"Initialized CPU monitor for {len(self.hostnames)} hosts")
        logger.info(f"Thresholds: CPU >= {self.cpu_threshold:.0f}%, Memory >= {self.memory_threshold:.0f}%")

    def _create_client(self) -> lark.Client:
        """
        Create and configure Lark client.

        Returns:
            Configured Lark client instance.
        """
        return create_feishu_client(
            self.config.cpu_monitor_app_id,
            self.config.cpu_monitor_app_secret
        )

    def _format_alert_message(self, cpu_data: Dict[str, CPUInfo]) -> str:
        """
        Format CPU/Memory monitoring data into Feishu alert message.

        Args:
            cpu_data: Dictionary mapping host names to CPUInfo objects.

        Returns:
            Formatted message string.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = [f"⚠️ CPU/Memory Alert [{timestamp}]\n"]

        has_alert = False

        for hostname, cpu_info in cpu_data.items():
            if cpu_info.cpu_usage >= self.cpu_threshold or cpu_info.memory_percent >= self.memory_threshold:
                has_alert = True
                lines.append(f"🚨 {hostname}:")
                lines.append(f"   {cpu_info}")

                if cpu_info.cpu_usage >= self.cpu_threshold:
                    lines.append(f"   ⚠️ CPU usage exceeds threshold ({self.cpu_threshold:.0f}%)")
                if cpu_info.memory_percent >= self.memory_threshold:
                    lines.append(f"   ⚠️ Memory usage exceeds threshold ({self.memory_threshold:.0f}%)")
                lines.append("")

        if not has_alert:
            lines.append("✅ All hosts within normal limits")

        return "\n".join(lines)

    def _format_status_message(self, cpu_data: Dict[str, CPUInfo]) -> str:
        """
        Format CPU/Memory monitoring data into Feishu status message.

        Args:
            cpu_data: Dictionary mapping host names to CPUInfo objects.

        Returns:
            Formatted message string.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = [f"📊 CPU/Memory Status Report [{timestamp}]\n"]

        for hostname, cpu_info in cpu_data.items():
            lines.append(f"{hostname}:")
            lines.append(f"  {cpu_info}")
            lines.append("")

        return "\n".join(lines)

    def send_message(self, message: str) -> bool:
        """
        Send message to Feishu chat.

        Args:
            message: Message content to send.

        Returns:
            True if message sent successfully, False otherwise.
        """
        return send_feishu_message(
            message_content=message,
            client=self.client,
            chat_id=self.config.cpu_monitor_chat_id,
        )

    def run(self) -> None:
        """Execute CPU/Memory monitoring and send alert only if threshold exceeded (single-shot mode)."""
        logger.info("Starting CPU/Memory monitoring (single-shot mode)...")

        try:
            # Initialize CPU monitor
            cpu_monitor = CPUMonitor(self.hostnames)

            # Monitor all hosts
            cpu_data = cpu_monitor.monitor_all()

            if not cpu_data:
                logger.warning("No CPU/Memory data collected from any host")
                self.send_message("CPU/Memory Monitoring Failed: No data collected from any host")
                return

            # Check if any host exceeds thresholds
            has_alert = any(
                info.cpu_usage >= self.cpu_threshold or info.memory_percent >= self.memory_threshold
                for info in cpu_data.values()
            )

            if has_alert:
                message = self._format_alert_message(cpu_data)
                logger.warning("Threshold exceeded, sending alert message")
                self.send_message(message)
            else:
                logger.info("All hosts within limits, no message sent")

        except Exception as e:
            logger.error(f"CPU/Memory monitoring failed: {e}", exc_info=True)
            raise

    def run_continuous(self, check_interval: int = 5) -> None:
        """
        Run CPU/Memory monitoring continuously, checking every N seconds.

        Args:
            check_interval: Time in seconds between checks (default: 5).
        """
        logger.info(f"Starting CPU/Memory monitoring (continuous mode, checking every {check_interval}s)...")

        try:
            # Initialize CPU monitor
            cpu_monitor = CPUMonitor(self.hostnames)

            logger.info("Monitoring CPU/Memory continuously. Press Ctrl+C to stop.")

            while True:
                try:
                    # Monitor all hosts
                    cpu_data = cpu_monitor.monitor_all()

                    if not cpu_data:
                        logger.warning("No CPU/Memory data collected from any host")
                    else:
                        # Check if any host exceeds thresholds
                        has_alert = any(
                            info.cpu_usage >= self.cpu_threshold or info.memory_percent >= self.memory_threshold
                            for info in cpu_data.values()
                        )

                        if has_alert:
                            message = self._format_alert_message(cpu_data)
                            logger.warning("Threshold exceeded, sending alert message")
                            self.send_message(message)
                        else:
                            logger.debug("All hosts within limits")

                    # Wait before next check
                    logger.debug(f"Waiting {check_interval}s before next check...")
                    time.sleep(check_interval)

                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, stopping continuous monitoring...")
                    break
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                    # Continue monitoring despite errors
                    time.sleep(check_interval)

        except Exception as e:
            logger.error(f"Continuous CPU/Memory monitoring failed: {e}", exc_info=True)
            raise