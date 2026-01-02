import logging
import sys

def setup_logging(level: int = logging.INFO) -> None:
    """Configures centralized logging for PSPF."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

def get_logger(name: str) -> logging.Logger:
    """Returns a logger instance for a given component."""
    return logging.getLogger(f"pspf.{name}")
