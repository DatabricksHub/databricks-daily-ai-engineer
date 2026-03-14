"""
agents/base_agent.py — Abstract base class for every agent in the pipeline.

Every agent:
  - Has a unique name and a run() method.
  - Emits structured AgentResult objects so the supervisor can route decisions.
  - Logs all actions through the shared logger.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from utils.logger import get_logger


@dataclass
class AgentResult:
    """Standardised return value from every agent."""

    agent: str                          # Which agent produced this result
    success: bool                       # Did the agent complete its task?
    data: dict[str, Any] = field(default_factory=dict)   # Payload for downstream agents
    error: str = ""                     # Human-readable error if success=False
    duration_seconds: float = 0.0      # How long the agent took


class BaseAgent(ABC):
    """
    All agents inherit from this class.

    Subclasses must implement `_run(context)` — the supervisor calls `run()`
    which wraps _run with timing, logging, and error handling.
    """

    def __init__(self, name: str):
        self.name = name
        self.logger = get_logger(f"agents.{name.lower().replace(' ', '_')}", name)

    def run(self, context: dict[str, Any]) -> AgentResult:
        """
        Public entry point called by the supervisor.
        Wraps _run() with timing and error handling.
        """
        self.logger.info(f"▶  Starting")
        start = time.monotonic()

        try:
            result = self._run(context)
            result.duration_seconds = time.monotonic() - start

            if result.success:
                self.logger.info(
                    f"✅ Completed in {result.duration_seconds:.1f}s"
                )
            else:
                self.logger.error(
                    f"❌ Failed in {result.duration_seconds:.1f}s — {result.error}"
                )

            return result

        except Exception as exc:
            duration = time.monotonic() - start
            self.logger.exception(f"💥 Unhandled exception after {duration:.1f}s")
            return AgentResult(
                agent=self.name,
                success=False,
                error=str(exc),
                duration_seconds=duration,
            )

    @abstractmethod
    def _run(self, context: dict[str, Any]) -> AgentResult:
        """Implement the agent's core logic here."""
        ...
