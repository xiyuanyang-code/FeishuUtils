"""Configuration management for Feishu BookKeeping."""

import os
from pathlib import Path
from typing import Dict, Any

import yaml


class Config:
    """Manage application configuration from YAML file."""

    def __init__(self, config_path: str | None = None):
        """
        Initialize configuration.

        Args:
            config_path: Path to config.yaml file. If None, uses default path.
        """
        if config_path is None:
            project_root = Path(__file__).parent.parent
            config_path = project_root / "config" / "config.yaml"

        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, encoding="utf-8") as f:
            self._config = yaml.safe_load(f) or {}

    @property
    def app_id(self) -> str:
        """Get Feishu APP_ID."""
        return self._config.get("env", {}).get("APP_ID", "")

    @property
    def app_secret(self) -> str:
        """Get Feishu APP_SECRET."""
        return self._config.get("env", {}).get("APP_SECRET", "")

    @property
    def chat_id(self) -> str:
        """Get Feishu chat ID."""
        return self._config.get("env", {}).get("chat_id", "")
