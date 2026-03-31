"""Main entry point for Feishu monitoring application."""

import argparse
import logging
import sys

from src.config import Config
from src.bookkeeper import FeishuBookKeeper
from src.gpu_fetching import FeishuGPUMonitor


def setup_logging() -> None:
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_bookkeeping(config: Config) -> None:
    """
    Run bookkeeping task.

    Args:
        config: Configuration object.
    """
    logger = logging.getLogger(__name__)
    try:
        logger.info("Starting bookkeeping task...")
        bookkeeper = FeishuBookKeeper(config)
        bookkeeper.run(output_path="bookkeeping_records.csv")
    except Exception as e:
        logger.error(f"Bookkeeping task failed: {e}", exc_info=True)
        raise


def run_gpu_monitor(config: Config, run_once: bool = False, continue_run: bool = False) -> None:
    """
    Run GPU monitoring task.

    Args:
        config: Configuration object.
        run_once: If True, run once and exit.
        continue_run: If True, run continuously and check for new messages.
    """
    logger = logging.getLogger(__name__)
    try:
        logger.info("Starting GPU monitoring task...")
        gpu_monitor = FeishuGPUMonitor(config)

        if run_once:
            logger.info("Running in single-shot mode")
            gpu_monitor.run()
        elif continue_run:
            logger.info("Running in continuous mode")
            gpu_monitor.run_continuous(check_interval=5)
        else:
            logger.warning("No GPU mode specified, defaulting to single-shot")
            gpu_monitor.run()

    except Exception as e:
        logger.error(f"GPU monitoring task failed: {e}", exc_info=True)
        raise


def main() -> None:
    """Run the Feishu monitoring application."""
    setup_logging()
    logger = logging.getLogger(__name__)

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Feishu monitoring application"
    )
    parser.add_argument(
        "--task_type",
        choices=["book", "gpu"],
        required=True,
        help="Task type to run: 'book' for bookkeeping, 'gpu' for GPU monitoring"
    )

    # GPU monitoring mode options (only for GPU task)
    parser.add_argument(
        "--run_once",
        action="store_true",
        help="Run GPU monitoring once and exit"
    )
    parser.add_argument(
        "--continue_run",
        action="store_true",
        help="Run GPU monitoring continuously, checking for new messages every 30s"
    )

    args = parser.parse_args()

    # Validate GPU mode options
    if args.task_type == "gpu":
        if args.run_once and args.continue_run:
            parser.error("Cannot specify both --run_once and --continue_run")
        if not args.run_once and not args.continue_run:
            parser.error("Must specify either --run_once or --continue_run for GPU task")

    try:
        # Load configuration
        config = Config()
        logger.info("Configuration loaded successfully")

        # Route to appropriate task
        if args.task_type == "book":
            run_bookkeeping(config)
        elif args.task_type == "gpu":
            run_gpu_monitor(config, run_once=args.run_once, continue_run=args.continue_run)

    except Exception as e:
        logger.error(f"Application failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
