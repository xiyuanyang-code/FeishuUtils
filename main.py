"""Main entry point for Feishu BookKeeping application."""

import logging

from src.config import Config
from src.bookkeeper import FeishuBookKeeper


def setup_logging() -> None:
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    """Run the Feishu BookKeeping application."""
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = Config()
        logger.info("Configuration loaded successfully")

        # Initialize bookkeeper
        bookkeeper = FeishuBookKeeper(config)

        # Run bookkeeping process
        bookkeeper.run(output_path="bookkeeping_records.csv")

    except Exception as e:
        logger.error(f"Application failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
