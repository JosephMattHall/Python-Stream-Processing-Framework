import logging
import sys
import json
import datetime
from typing import Any, Dict

class JSONFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings for structured logging.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": datetime.datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }

        # Include exception info if present
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        
        # Include stack info if present
        if record.stack_info:
            log_record["stack_trace"] = self.formatStack(record.stack_info)

        return json.dumps(log_record)

def setup_logging(level: int = logging.INFO) -> None:
    """Configures centralized JSON structured logging for PSPF."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates if this is called multiple times
    if root_logger.handlers:
         root_logger.handlers.clear()
         
    root_logger.addHandler(handler)

def get_logger(name: str) -> logging.Logger:
    """Returns a logger instance for a given component."""
    return logging.getLogger(f"pspf.{name}")
