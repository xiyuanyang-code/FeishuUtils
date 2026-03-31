"""Feishu BookKeeping main logic."""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    ListMessageRequest,
    ListMessageResponse,
    CreateMessageRequest,
    CreateMessageResponse,
    CreateMessageRequestBody,
)

from src.config import Config

logger = logging.getLogger(__name__)


class Record:
    """Represent a single bookkeeping record."""

    def __init__(
        self,
        timestamp: str,
        amount: float,
        category: str,
        reason: str,
        sender: str,
    ):
        """
        Initialize a bookkeeping record.

        Args:
            timestamp: Formatted timestamp string.
            amount: Transaction amount.
            category: Category (e.g., "吃饭", "零食").
            reason: Detailed reason.
            sender: Message sender name.
        """
        self.timestamp = timestamp
        self.amount = amount
        self.category = category
        self.reason = reason
        self.sender = sender

    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary."""
        return {
            "时间": self.timestamp,
            "金额": self.amount,
            "大类": self.category,
            "具体原因": self.reason,
            "发送者": self.sender,
        }


class FeishuBookKeeper:
    """Manage Feishu message extraction and bookkeeping."""

    def __init__(self, config: Config):
        """
        Initialize Feishu BookKeeper.

        Args:
            config: Configuration object.
        """
        self.config = config
        self.client = self._create_client()

    def _create_client(self) -> lark.Client:
        """
        Create and configure Lark client.

        Returns:
            Configured Lark client instance.
        """
        return (
            lark.Client.builder()
            .app_id(self.config.bookkeeping_app_id)
            .app_secret(self.config.bookkeeping_app_secret)
            .log_level(lark.LogLevel.INFO)
            .build()
        )

    def _format_timestamp(self, timestamp_ms: str) -> str:
        """
        Convert millisecond timestamp to readable format.

        Args:
            timestamp_ms: Timestamp in milliseconds.

        Returns:
            Formatted datetime string (YYYY-MM-DD HH:MM:SS).
        """
        try:
            timestamp_sec = int(timestamp_ms) / 1000
            dt = datetime.fromtimestamp(timestamp_sec)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OSError):
            return timestamp_ms

    def _parse_text_message(self, content: str) -> Optional[Record]:
        """
        Parse text message content into bookkeeping record.

        Expected format: "整数金额 大类原因 具体原因"
        Example: "20 吃饭 陈香贵牛肉面"

        The first space-separated part must be an integer.

        Args:
            content: Message text content.

        Returns:
            Record object if parsing succeeds, None otherwise.
        """
        try:
            # Parse JSON content
            content_dict = json.loads(content)
            text = content_dict.get("text", "").strip()

            # Find first space
            first_space_idx = text.find(" ")
            if first_space_idx == -1:
                return None

            # Extract first part (before first space)
            first_part = text[:first_space_idx].strip()
            remaining_text = text[first_space_idx + 1:].strip()

            # First part must be an integer
            try:
                amount = int(first_part)
                if amount < 0:
                    return None
            except ValueError:
                return None

            # Parse remaining text for category and reason
            remaining_parts = remaining_text.split(maxsplit=1)
            if not remaining_parts:
                return None

            category = remaining_parts[0]
            reason = remaining_parts[1] if len(remaining_parts) > 1 else ""

            return Record(
                timestamp="",
                amount=float(amount),
                category=category,
                reason=reason,
                sender="",
            )
        except (json.JSONDecodeError, ValueError):
            return None

    def _extract_sender_name(self, sender: Dict[str, Any]) -> str:
        """
        Extract sender name from sender info.

        Args:
            sender: Sender dictionary from API response.

        Returns:
            Sender name or ID.
        """
        sender_type = sender.get("sender_type", "")
        sender_id = sender.get("id", "")

        if sender_type == "user" and sender_id:
            return f"User({sender_id})"
        return sender_type or "Unknown"

    def _extract_sender_name_from_object(self, sender: Any) -> str:
        """
        Extract sender name from Sender object.

        Args:
            sender: Sender object from API response.

        Returns:
            Sender name or ID.
        """
        sender_type = getattr(sender, "sender_type", "")
        sender_id = getattr(sender, "id", "")

        if sender_type == "user" and sender_id:
            return f"User({sender_id})"
        return sender_type or "Unknown"

    def fetch_messages(self) -> List[Any]:
        """
        Fetch messages from Feishu chat.

        Returns:
            List of Message objects.
        """
        request: ListMessageRequest = (
            ListMessageRequest.builder()
            .container_id_type("chat")
            .container_id(self.config.bookkeeping_chat_id)
            .sort_type("ByCreateTimeAsc")
            .page_size(50)
            .build()
        )

        response: ListMessageResponse = self.client.im.v1.message.list(request)

        if not response.success():
            logger.error(
                f"Failed to fetch messages: code={response.code}, msg={response.msg}"
            )
            if response.raw:
                logger.error(f"Response raw: {response.raw.content[:500]}")
            return []

        messages = response.data.items or []
        logger.info(f"Fetched {len(messages)} messages")
        return messages

    def extract_records(self, messages: List[Any]) -> List[Record]:
        """
        Extract bookkeeping records from messages.

        Args:
            messages: List of Message objects.

        Returns:
            List of Record objects.
        """
        records = []

        for msg in messages:
            # Skip non-text messages
            if msg.msg_type != "text":
                continue

            # Parse message content
            content = msg.body.content if msg.body else ""

            parsed = self._parse_text_message(content)
            if parsed is None:
                continue

            # Update timestamp and sender
            parsed.timestamp = self._format_timestamp(msg.create_time)
            parsed.sender = self._extract_sender_name_from_object(msg.sender)

            records.append(parsed)

        logger.info(f"Extracted {len(records)} valid records")
        return records

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
                .container_id(self.config.bookkeeping_chat_id)
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

    def extract_records_after_timestamp(
        self, messages: List[Any], start_timestamp: Optional[str]
    ) -> List[Record]:
        """
        Extract bookkeeping records after a specific timestamp.

        Args:
            messages: List of Message objects.
            start_timestamp: Start timestamp (exclusive). If None, extract all.

        Returns:
            List of Record objects.
        """
        records = []

        for msg in messages:
            # Skip non-text messages
            if msg.msg_type != "text":
                continue

            # Filter by timestamp if provided
            if start_timestamp:
                try:
                    msg_time = int(msg.create_time)
                    start_time = int(start_timestamp)
                    if msg_time <= start_time:
                        continue
                except ValueError:
                    logger.warning(f"Invalid timestamp format: {msg.create_time}")
                    continue

            # Parse message content
            content = msg.body.content if msg.body else ""

            parsed = self._parse_text_message(content)
            if parsed is None:
                continue

            # Update timestamp and sender
            parsed.timestamp = self._format_timestamp(msg.create_time)
            parsed.sender = self._extract_sender_name_from_object(msg.sender)

            records.append(parsed)

        logger.info(f"Extracted {len(records)} valid records after timestamp")
        return records

    def _export_to_result_structure(self, records: List[Record]) -> None:
        """
        Export records to result structure (global.csv + timestamped folder).

        Args:
            records: List of Record objects to export.
        """
        result_dir = Path("result")
        result_dir.mkdir(exist_ok=True)

        # 1. Append to global.csv
        global_csv = result_dir / "global.csv"
        self._append_to_csv(records, global_csv)

        # 2. Create timestamped subfolder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamp_dir = result_dir / timestamp
        timestamp_dir.mkdir(exist_ok=True)

        # 3. Export to timestamped/result.csv
        timestamp_csv = timestamp_dir / "result.csv"
        self._write_csv(records, timestamp_csv)

        logger.info(f"Exported to {global_csv} and {timestamp_csv}")

    def _append_to_csv(self, records: List[Record], csv_path: Path) -> None:
        """
        Append records to CSV file.

        Args:
            records: List of Record objects.
            csv_path: Path to CSV file.
        """
        file_exists = csv_path.exists()

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["时间", "金额", "大类", "具体原因", "发送者"])

            # Write header if file is new
            if not file_exists:
                writer.writeheader()

            # Append records
            for record in records:
                writer.writerow(record.to_dict())

    def _write_csv(self, records: List[Record], csv_path: Path) -> None:
        """
        Write records to CSV file (overwrite).

        Args:
            records: List of Record objects.
            csv_path: Path to CSV file.
        """
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["时间", "金额", "大类", "具体原因", "发送者"])
            writer.writeheader()
            for record in records:
                writer.writerow(record.to_dict())

    def export_to_csv(self, records: List[Record], output_path: str) -> None:
        """
        Export records to CSV file.

        Args:
            records: List of Record objects.
            output_path: Path to output CSV file.
        """
        if not records:
            logger.warning("No records to export")
            return

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["时间", "金额", "大类", "具体原因", "发送者"])
            writer.writeheader()
            for record in records:
                writer.writerow(record.to_dict())

        logger.info(f"Exported {len(records)} records to {output_file}")

    def _calculate_total_amount(self, records: List[Record]) -> float:
        """
        Calculate total amount from records.

        Args:
            records: List of Record objects.

        Returns:
            Total amount.
        """
        return sum(record.amount for record in records)

    def send_success_message(self, total_amount: float) -> bool:
        """
        Send success message with total amount to Feishu chat.

        Args:
            total_amount: Total consumption amount.

        Returns:
            True if message sent successfully, False otherwise.
        """
        try:
            # Create message content
            content = json.dumps({"text": f"Successful！\n总消费金额：{total_amount} 元"})

            # Build request
            request: CreateMessageRequest = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(self.config.bookkeeping_chat_id)
                    .msg_type("text")
                    .content(content)
                    .build()
                )
                .build()
            )

            # Send message
            response: CreateMessageResponse = self.client.im.v1.message.create(request)

            if not response.success():
                logger.error(
                    f"Failed to send message: code={response.code}, msg={response.msg}"
                )
                return False

            logger.info("Success message sent to chat")
            return True

        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def run(self, output_path: str = "bookkeeping_records.csv") -> None:
        """
        Run the bookkeeping process.

        Args:
            output_path: Path to output CSV file (deprecated, using result/ structure).
        """
        logger.info("Starting Feishu BookKeeping...")

        # Get timestamp of last bot message
        last_bot_message_time = self._get_last_bot_message_timestamp()
        logger.info(f"Last bot message timestamp: {last_bot_message_time or 'None (first run)'}")

        # Fetch and extract records after last bot message
        messages = self.fetch_messages()
        records = self.extract_records_after_timestamp(messages, last_bot_message_time)

        if records:
            # Export to result structure
            self._export_to_result_structure(records)

            # Calculate total amount and send success message
            total_amount = self._calculate_total_amount(records)
            self.send_success_message(total_amount)
        else:
            logger.info("No new records found since last bot message")

        logger.info("BookKeeping completed successfully")
