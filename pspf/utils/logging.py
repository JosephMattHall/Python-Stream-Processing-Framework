import logging
import sys
import json
import datetime
import os
import contextvars
from typing import Any, Dict

# Global context for structured logging
log_context: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar("log_context", default={})

def bind_context(**kwargs: Any) -> contextvars.Token:
    """Binds variables to the current logging context."""
    ctx = log_context.get().copy()
    ctx.update(kwargs)
    return log_context.set(ctx)

def reset_context(token: contextvars.Token) -> None:
    """Resets the context to the previous state."""
    log_context.reset(token)

def clear_context() -> None:
    """Clears the logging context."""
    log_context.set({})

class JSONFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings for structured logging.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
            "pid": os.getpid(),
        }

        # Include contextual variables
        ctx_data = log_context.get()
        if ctx_data:
            log_record.update(ctx_data)

        # Include custom attributes from 'extra'
        if hasattr(record, 'extra'):
             log_record.update(record.extra) # type: ignore

        # Include exception info if present
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info) # type: ignore
        
        # Include stack info if present
        if record.stack_info:
            log_record["stack_trace"] = self.formatStack(record.stack_info) # type: ignore

        return json.dumps(log_record)

class ConsoleFormatter(logging.Formatter):
    """
    Human-readable formatter for development.
    """
    def format(self, record: logging.LogRecord) -> str:
        # 2023-10-27T10:00:00 [INFO] [logger] message
        timestamp = datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%dT%H:%M:%S')
        return f"{timestamp} [{record.levelname}] [{record.name}] {record.getMessage()}"

def setup_logging(level: int = logging.INFO) -> None:
    """
    Configures centralized logging for PSPF.
    Respects LOG_FORMAT environment variable (json/text).
    """
    log_format = os.getenv("LOG_FORMAT", "text").lower()
    
    handler = logging.StreamHandler(sys.stdout)
    
    if log_format == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(ConsoleFormatter())
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    if root_logger.handlers:
         root_logger.handlers.clear()
         
    root_logger.addHandler(handler)
    
    # Silence noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    """Returns a logger instance for a given component."""
    return logging.getLogger(f"pspf.{name}")
