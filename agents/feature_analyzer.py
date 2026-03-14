"""
agents/feature_analyzer.py — Feature Analyzer Agent

Responsibility:
    Read the article list from the Knowledge Agent and the feature history,
    then ask Claude to pick the most interesting Databricks feature that
    hasn't been demonstrated yet and suggest a concrete demo project idea.

Output data keys:
    feature      — short name, e.g. "Lakebase"
    description  — one-sentence description of the feature
    project_idea — concrete demo project idea string
    tags         — list[str] of related keywords
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from agents.base_agent import BaseAgent, AgentResult
from config import cfg
from utils.claude_client import chat


_SYSTEM_PROMPT = """
You are an expert Databricks data engineer and technical writer.
Your job is to analyse a list of Databricks articles and select the SINGLE
most interesting and recently discussed feature to demonstrate next.

Rules:
1. Never pick a feature from the "already demonstrated" history list.
2. Prefer genuinely new or recently announced capabilities.
3. Choose something that can be demonstrated with a single self-contained
   Databricks notebook (no external dependencies beyond the workspace).
4. Respond ONLY with valid JSON — no markdown fences, no explanation.
""".strip()


class FeatureAnalyzerAgent(BaseAgent):
    """Uses Claude to select the next Databricks feature to demonstrate."""

    def __init__(self):
        super().__init__("Feature Analyzer")

    # ── Public interface ───────────────────────────────────────────────────────

    def _run(self, context: dict[str, Any]) -> AgentResult:
        articles: list[dict] = context.get("articles", [])
        if not articles:
            return AgentResult(
                agent=self.name,
                success=False,
                error="No articles provided — Knowledge Agent may have failed.",
            )

        history = self._load_history()
        self.logger.info(
            f"Analysing {len(articles)} articles. "
            f"Already demonstrated: {len(history)} features."
        )

        prompt = self._build_prompt(articles, history)
        raw_response = chat(
            prompt=prompt,
            system=_SYSTEM_PROMPT,
            max_tokens=1024,
            temperature=0.4,
        )

        parsed = self._parse_response(raw_response)
        if parsed is None:
            return AgentResult(
                agent=self.name,
                success=False,
                error=f"Could not parse Claude response: {raw_response[:200]}",
            )

        # Persist the new feature to history immediately
        history.append(parsed["feature"])
        self._save_history(history)
        self.logger.info(
            f"Selected feature: '{parsed['feature']}' — {parsed['project_idea']}"
        )

        return AgentResult(
            agent=self.name,
            success=True,
            data=parsed,
        )

    # ── Private helpers ────────────────────────────────────────────────────────

    def _build_prompt(self, articles: list[dict], history: list[str]) -> str:
        # Format the article list for Claude
        articles_text = "\n".join(
            f"- [{a['source']}] {a['title']}: {a['snippet'][:150]}"
            for a in articles[:40]          # Limit to avoid token overflow
        )
        history_text = ", ".join(history) if history else "none yet"

        return f"""
Here are the latest Databricks articles I found:

{articles_text}

Already demonstrated features (DO NOT repeat these):
{history_text}

Pick the SINGLE best feature to demonstrate next. Respond with this exact JSON:
{{
  "feature": "<short feature name, e.g. 'Lakebase'>",
  "description": "<one sentence describing what this feature does>",
  "project_idea": "<concrete demo project, e.g. 'Real-time inventory tracking using streaming ingestion'>",
  "tags": ["<tag1>", "<tag2>", "<tag3>"]
}}
""".strip()

    def _parse_response(self, response: str) -> dict | None:
        """Extract and validate the JSON object from Claude's response."""
        # Strip markdown code fences if present
        text = response.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            text = "\n".join(
                line for line in lines if not line.startswith("```")
            ).strip()

        try:
            data = json.loads(text)
            required_keys = {"feature", "description", "project_idea", "tags"}
            if not required_keys.issubset(data.keys()):
                self.logger.warning(f"Response missing keys: {required_keys - data.keys()}")
                return None
            return data
        except json.JSONDecodeError as exc:
            self.logger.warning(f"JSON parse error: {exc}")
            return None

    def _load_history(self) -> list[str]:
        path = Path(cfg.history_file)
        if path.exists():
            try:
                return json.loads(path.read_text())
            except (json.JSONDecodeError, IOError):
                self.logger.warning("history.json is corrupt — starting fresh.")
        return []

    def _save_history(self, history: list[str]) -> None:
        path = Path(cfg.history_file)
        path.write_text(json.dumps(history, indent=2), encoding='utf-8')
        self.logger.debug(f"History saved ({len(history)} entries) → {path}")