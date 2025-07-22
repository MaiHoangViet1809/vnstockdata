"""
Central log-configuration helper.
Keeps one rotating handler alive for the whole process.
"""

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import sys

__all__ = ["configure_logging"]

# --- private -----------------------------------------------------------------
_LOG_INITIALIZED = False          # module-level guard


def _make_handler(log_file: Path,
                  when: str = "midnight",
                  backup_count: int = 7) -> TimedRotatingFileHandler:
    """Create a daily rotating file handler."""
    log_file.parent.mkdir(parents=True, exist_ok=True)

    handler = TimedRotatingFileHandler(
        filename=log_file,
        when=when,               # rotate at midnight local time
        interval=1,
        backupCount=backup_count,
        encoding="utf-8",
        utc=False,               # change to True if you log in UTC
    )
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    return handler


# --- public ------------------------------------------------------------------
def configure_logging(base_path: str | Path,
                      *,
                      level: int = logging.INFO,
                      logger_name: str | None = None,
                      when: str = "midnight",
                      backup_count: int = 7) -> logging.Logger:
    """
    Attach a TimedRotatingFileHandler to `logger_name` (root by default).

    Safe to call repeatedly.
    Raises only on truly fatal conditions (e.g. unwritable directory); in that
    case it falls back to STDERR so your app keeps running.
    """
    global _LOG_INITIALIZED
    logger = logging.getLogger(logger_name)

    if _LOG_INITIALIZED:     # Already configured in this process
        return logger

    try:
        base_path = Path(base_path).expanduser().resolve()
        log_file = base_path / "logs" / "scheduler.log"
        handler = _make_handler(log_file, when, backup_count)

        logger.setLevel(level)
        # avoid double attachment in edge-cases (e.g. gunicorn forking)
        if not any(isinstance(h, TimedRotatingFileHandler) for h in logger.handlers):
            logger.addHandler(handler)
    except Exception as exc:     # permissions, disk full, etc.
        # Fallback: simple console logging so the process still speaks
        logging.basicConfig(
            stream=sys.stderr,
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        logger.error("LOGGING FELL BACK TO STDERR: %s", exc, exc_info=False)

    _LOG_INITIALIZED = True
    return logger
