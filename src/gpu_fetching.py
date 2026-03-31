"""GPU monitoring and Feishu notification."""

import json
import logging
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageResponse,
    CreateMessageRequestBody,
    ListMessageRequest,
    ListMessageResponse,
)

from src.config import Config

logger = logging.getLogger(__name__)


@dataclass
class GPUInfo:
    """Represent GPU information."""

    hostname: str
    gpu_name: str
    gpu_id: int
    utilization: float
    memory_used: int
    memory_total: int
    temperature: Optional[int] = None
    processes: List[Dict[str, Any]] = None

    def __init__(
        self,
        hostname: str,
        gpu_name: str,
        gpu_id: int,
        utilization: float,
        memory_used: int,
        memory_total: int,
        temperature: Optional[int] = None,
        processes: Optional[List[Dict[str, Any]]] = None,
    ):
        """
        Initialize GPU info.

        Args:
            hostname: Host name where GPU is located.
            gpu_name: GPU model name.
            gpu_id: GPU ID.
            utilization: GPU utilization percentage (0-100).
            memory_used: Used memory in MB.
            memory_total: Total memory in MB.
            temperature: GPU temperature in Celsius (optional).
            processes: List of processes using GPU (optional).
        """
        self.hostname = hostname
        self.gpu_name = gpu_name
        self.gpu_id = gpu_id
        self.utilization = utilization
        self.memory_used = memory_used
        self.memory_total = memory_total
        self.temperature = temperature
        self.processes = processes or []

    @property
    def memory_usage_percent(self) -> float:
        """Calculate memory usage percentage."""
        if self.memory_total == 0:
            return 0.0
        return (self.memory_used / self.memory_total) * 100

    def __str__(self) -> str:
        """Return formatted string representation."""
        process_info = ""
        if self.processes:
            process_info = f" ({len(self.processes)} processes)"

        temp_info = ""
        if self.temperature:
            temp_info = f" {self.temperature}C"

        return (
            f"GPU {self.gpu_id}: {self.gpu_name} "
            f"[{self.utilization:.0f}%] "
            f"{self.memory_used}/{self.memory_total} MB "
            f"({self.memory_usage_percent:.0f}%){temp_info}{process_info}"
        )


class GPUMonitor:
    """Monitor GPU status on remote servers."""

    def __init__(self, hostnames: List[str], timeout: int = 30):
        """
        Initialize GPU monitor.

        Args:
            hostnames: List of host names from SSH config.
            timeout: SSH connection timeout in seconds.
        """
        self.hostnames = hostnames
        self.timeout = timeout

    def _execute_gpustat_remote(self, hostname: str) -> Optional[str]:
        """
        Execute gpustat command on remote host.

        Args:
            hostname: Host name from SSH config (SSH will auto-resolve).

        Returns:
            Command output as string, or None if failed.
        """
        try:
            # Build SSH command with virtual environment activation
            # * you can switch to your own commands
            remote_command = "cd /data/xiyuanyang/public && source .venv/bin/activate && gpustat --json"

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
                logger.error(f"gpustat failed on {hostname}: {result.stderr}")
                return None

            return result.stdout

        except subprocess.TimeoutExpired:
            logger.error(f"SSH connection timeout to {hostname}")
            return None
        except Exception as e:
            logger.error(f"Error executing gpustat on {hostname}: {e}")
            return None

    def _parse_gpustat_json(self, output: str, hostname: str) -> List[GPUInfo]:
        """
        Parse gpustat JSON output.

        Args:
            output: JSON output from gpustat command.
            hostname: Host name where GPUs are located.

        Returns:
            List of GPUInfo objects.
        """
        try:
            data = json.loads(output)
            gpu_list = []

            for gpu_data in data.get("gpus", []):
                gpu = GPUInfo(
                    hostname=hostname,
                    gpu_name=gpu_data.get("name", "Unknown"),
                    gpu_id=gpu_data.get("index", 0),
                    utilization=gpu_data.get("utilization.gpu", 0.0),
                    memory_used=gpu_data.get("memory.used", 0),
                    memory_total=gpu_data.get("memory.total", 1),
                    temperature=gpu_data.get("temperature.gpu"),
                )

                # Parse processes if available
                processes = []
                for proc in gpu_data.get("processes", []):
                    processes.append(
                        {
                            "pid": proc.get("pid"),
                            "name": proc.get("name"),
                            "memory": proc.get("gpu_memory_usage"),
                        }
                    )
                gpu.processes = processes

                gpu_list.append(gpu)

            return gpu_list

        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing gpustat JSON from {hostname}: {e}")
            return []

    def monitor_host(self, hostname: str) -> List[GPUInfo]:
        """
        Monitor GPUs on a specific host.

        Args:
            hostname: Host name from SSH config.

        Returns:
            List of GPUInfo objects.
        """
        logger.info(f"Monitoring GPU on {hostname}...")
        output = self._execute_gpustat_remote(hostname)

        if not output:
            logger.warning(f"No GPU data received from {hostname}")
            return []

        gpu_list = self._parse_gpustat_json(output, hostname)

        if gpu_list:
            logger.info(f"Found {len(gpu_list)} GPU(s) on {hostname}")
        else:
            logger.warning(f"No valid GPU data parsed from {hostname}")

        return gpu_list

    def monitor_all(self, max_workers: int = 5) -> Dict[str, List[GPUInfo]]:
        """
        Monitor GPUs on all hosts concurrently.

        Args:
            max_workers: Maximum number of concurrent SSH connections.

        Returns:
            Dictionary mapping host names to lists of GPUInfo objects.
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
                    gpu_list = future.result()
                    if gpu_list:
                        results[hostname] = gpu_list
                except Exception as e:
                    logger.error(f"Error monitoring {hostname}: {e}")

        return results


class FeishuGPUMonitor:
    """GPU monitoring with Feishu notifications."""

    def __init__(self, config: Config):
        """
        Initialize Feishu GPU monitor.

        Args:
            config: Configuration object.
        """
        self.config = config
        self.client = self._create_client()

        # Get GPU node names from config
        self.hostnames = config.gpu_node_names
        if not self.hostnames:
            raise ValueError("No GPU node names configured in config.yaml")

        logger.info(f"Initialized GPU monitor for {len(self.hostnames)} hosts")

    def _create_client(self) -> lark.Client:
        """
        Create and configure Lark client.

        Returns:
            Configured Lark client instance.
        """
        return (
            lark.Client.builder()
            .app_id(self.config.gpu_monitor_app_id)
            .app_secret(self.config.gpu_monitor_app_secret)
            .log_level(lark.LogLevel.INFO)
            .build()
        )

    def _format_gpu_message(self, gpu_data: Dict[str, List[GPUInfo]]) -> str:
        """
        Format GPU monitoring data into Feishu message.

        Args:
            gpu_data: Dictionary mapping host names to GPU info lists.

        Returns:
            Formatted message string.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines = [f"GPU Monitor Report [{timestamp}]\n"]

        total_gpus = 0
        total_utilization = 0.0

        for hostname, gpu_list in gpu_data.items():
            if not gpu_list:
                continue

            lines.append(f"{hostname}:")
            total_gpus += len(gpu_list)

            for gpu in gpu_list:
                lines.append(f"  GPU {gpu.gpu_id}: {gpu.gpu_name} [{gpu.utilization:.0f}%] {gpu.memory_used}/{gpu.memory_total} MB")
                total_utilization += gpu.utilization

            lines.append("")  # Empty line between hosts

        # Summary
        if total_gpus > 0:
            avg_utilization = total_utilization / total_gpus
            lines.append(f"Summary: {total_gpus} GPU(s), Avg Utilization: {avg_utilization:.1f}%")
        else:
            lines.append("No GPU data available")

        return "\n".join(lines)

    def send_message(self, message: str) -> bool:
        """
        Send message to Feishu chat.

        Args:
            message: Message content to send.

        Returns:
            True if message sent successfully, False otherwise.
        """
        try:
            content = json.dumps({"text": message})

            request: CreateMessageRequest = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(self.config.gpu_monitor_chat_id)
                    .msg_type("text")
                    .content(content)
                    .build()
                )
                .build()
            )

            response: CreateMessageResponse = self.client.im.v1.message.create(request)

            if not response.success():
                logger.error(
                    f"Failed to send message: code={response.code}, msg={response.msg}"
                )
                return False

            logger.info("GPU monitoring message sent successfully")
            return True

        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def run(self) -> None:
        """Execute GPU monitoring and send report to Feishu (single-shot mode)."""
        logger.info("Starting GPU monitoring (single-shot mode)...")

        try:
            # Initialize GPU monitor
            gpu_monitor = GPUMonitor(self.hostnames)

            # Monitor all hosts
            gpu_data = gpu_monitor.monitor_all()

            if not gpu_data:
                logger.warning("No GPU data collected from any host")
                # Still send a message about failure
                self.send_message("GPU Monitoring Failed: No data collected from any host")
                return

            # Format and send message
            message = self._format_gpu_message(gpu_data)
            success = self.send_message(message)

            if success:
                logger.info("GPU monitoring report sent successfully")
            else:
                logger.error("Failed to send GPU monitoring report")

        except Exception as e:
            logger.error(f"GPU monitoring failed: {e}", exc_info=True)
            raise

    def _get_last_bot_message_timestamp(self) -> Optional[str]:
        """
        Get the timestamp of the last message sent by the bot itself.

        Returns:
            Timestamp string of last bot message, or None if not found.
        """
        try:
            # Fetch messages to find bot messages
            request: ListMessageRequest = (
                ListMessageRequest.builder()
                .container_id_type("chat")
                .container_id(self.config.gpu_monitor_chat_id)
                .sort_type("ByCreateTimeDesc")
                .page_size(50)
                .build()
            )

            response: ListMessageResponse = self.client.im.v1.message.list(request)

            if not response.success():
                logger.warning(f"Failed to fetch bot messages: {response.msg}")
                return None

            messages = response.data.items or []

            # Find the most recent message sent by bot (sender_type = "app" or "bot")
            for msg in messages:
                if hasattr(msg, 'sender') and msg.sender:
                    sender_type = getattr(msg.sender, "sender_type", "")
                    if sender_type in ("app", "bot"):
                        return msg.create_time

            return None

        except Exception as e:
            logger.error(f"Error getting last bot message: {e}")
            return None

    def _has_new_user_messages(self, last_bot_timestamp: Optional[str]) -> bool:
        """
        Check if there are new user messages after the last bot message.

        Args:
            last_bot_timestamp: Timestamp of the last bot message.

        Returns:
            True if there are new user messages, False otherwise.
        """
        try:
            # Fetch recent messages
            request: ListMessageRequest = (
                ListMessageRequest.builder()
                .container_id_type("chat")
                .container_id(self.config.gpu_monitor_chat_id)
                .sort_type("ByCreateTimeDesc")
                .page_size(20)
                .build()
            )

            response: ListMessageResponse = self.client.im.v1.message.list(request)

            if not response.success():
                logger.warning(f"Failed to fetch messages: {response.msg}")
                return False

            messages = response.data.items or []

            # If no bot message exists, check for any user message
            if not last_bot_timestamp:
                for msg in messages:
                    if hasattr(msg, 'sender') and msg.sender:
                        sender_type = getattr(msg.sender, "sender_type", "")
                        if sender_type == "user":
                            return True
                return False

            # Check for user messages after last bot message
            for msg in messages:
                if hasattr(msg, 'sender') and msg.sender:
                    sender_type = getattr(msg.sender, "sender_type", "")
                    if sender_type == "user":
                        # Compare timestamps
                        try:
                            msg_time = int(msg.create_time)
                            bot_time = int(last_bot_timestamp)
                            if msg_time > bot_time:
                                return True
                        except ValueError:
                            logger.warning(f"Invalid timestamp format: {msg.create_time}")
                            continue

            return False

        except Exception as e:
            logger.error(f"Error checking for new messages: {e}")
            return False

    def run_continuous(self, check_interval: int = 30) -> None:
        """
        Run GPU monitoring continuously, checking for new messages periodically.

        Args:
            check_interval: Time in seconds between checks (default: 30).
        """
        logger.info(f"Starting GPU monitoring (continuous mode, checking every {check_interval}s)...")

        try:
            # Initialize GPU monitor
            gpu_monitor = GPUMonitor(self.hostnames)

            logger.info("Monitoring for new user messages. Press Ctrl+C to stop.")

            while True:
                try:
                    # Get timestamp of last bot message
                    last_bot_timestamp = self._get_last_bot_message_timestamp()
                    logger.debug(f"Last bot message timestamp: {last_bot_timestamp or 'None'}")

                    # Check for new user messages
                    has_new = self._has_new_user_messages(last_bot_timestamp)

                    if has_new:
                        logger.info("New user message detected, sending GPU status...")

                        # Monitor all hosts
                        gpu_data = gpu_monitor.monitor_all()

                        if gpu_data:
                            # Format and send message
                            message = self._format_gpu_message(gpu_data)
                            self.send_message(message)
                        else:
                            logger.warning("No GPU data collected")
                            self.send_message("GPU Monitoring Failed: No data collected")
                    else:
                        logger.debug("No new user messages detected")

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
            logger.error(f"Continuous GPU monitoring failed: {e}", exc_info=True)
            raise
