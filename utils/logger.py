"""
utils/logger.py — Structured, coloured console logger used by all agents.
"""

import logging
import sys
from datetime import datetime


COLORS = {
    "DEBUG":    "\033[36m",   # Cyan
    "INFO":     "\033[32m",   # Green
    "WARNING":  "\033[33m",   # Yellow
    "ERROR":    "\033[31m",   # Red
    "CRITICAL": "\033[35m",   # Magenta
    "RESET":    "\033[0m",
    "BOLD":     "\033[1m",
    "AGENT":    "\033[34m",   # Blue — agent name
}


class AgentFormatter(logging.Formatter):
    """Custom formatter that adds colour and agent context."""

    def format(self, record: logging.LogRecord) -> str:
        level_color = COLORS.get(record.levelname, COLORS["RESET"])
        reset = COLORS["RESET"]
        bold = COLORS["BOLD"]
        agent_color = COLORS["AGENT"]

        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        agent = getattr(record, "agent", "system")

        return (
            f"{bold}{ts}{reset} "
            f"{level_color}[{record.levelname:8s}]{reset} "
            f"{agent_color}[{agent:25s}]{reset} "
            f"{record.getMessage()}"
        )


def get_logger(name: str, agent_name: str = "") -> logging.Logger:
    """Return a logger pre-configured with the agent formatter."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(AgentFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

    # Attach agent name to every record
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.agent = agent_name or name
        return record

    logging.setLogRecordFactory(record_factory)
    return logger
