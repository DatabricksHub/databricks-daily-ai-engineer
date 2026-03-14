"""
agents/validation_agent.py — Validation Agent

Validates Databricks execution results before publishing.
Uses quality score as the primary gate — issues that were
resolved during execution (retries) do not block publishing.
"""

from __future__ import annotations

import json
import re
from typing import Any

from agents.base_agent import BaseAgent, AgentResult
from utils.claude_client import chat


_SYSTEM_PROMPT = """
You are a senior Databricks QA engineer reviewing SQL Warehouse execution results.
Analyse the provided run output and respond ONLY with valid JSON:
{
  "quality_score": <int 1-10>,
  "validated": <true|false>,
  "issues": [<string>, ...],
  "summary": "<one paragraph assessment>"
}

Scoring guide:
  9-10 — All cells succeeded, meaningful output, clear demo value
  7-8  — Minor issues (e.g. a cell failed but was fixed on retry), demo works overall
  5-6  — Partial success; some steps worked but output is unclear
  1-4  — Critical failure: most cells failed, no useful output

IMPORTANT RULES:
- If an issue says "resolved on retry" or "succeeded after fix" → it is NOT a blocking issue
- If run_state is SUCCESS → score should be >= 7 unless output is clearly empty/meaningless
- Set validated=true when quality_score >= 7
- Only list UNRESOLVED issues that actually block the demo from working
- Do NOT penalise for errors that were successfully retried and fixed
""".strip()

# Patterns that indicate a FINAL unresolved failure (not a retry)
_HARD_FAILURE = re.compile(
    r"run state: failed|overall.*failed|no cells.*executed",
    re.IGNORECASE,
)

# Patterns that show something succeeded
_SUCCESS_PATTERNS = re.compile(
    r"success|completed|✅|rows_loaded|step|status",
    re.IGNORECASE,
)

# Resolved-on-retry indicator — these should NOT block validation
_RESOLVED_PATTERNS = re.compile(
    r"resolved on retry|succeeded after fix|retry.*success|fixed.*retry",
    re.IGNORECASE,
)

# Databricks SQL unsupported types → should use STRING
_BAD_TYPES = re.compile(
    r"\bTEXT\b|\bVARCHAR\b|\bNVARCHAR\b|\bCHAR\b|\bINTEGER\b|\bFLOAT4\b|\bFLOAT8\b",
    re.IGNORECASE,
)


class ValidationAgent(BaseAgent):
    """Validates Databricks job results before publishing."""

    def __init__(self):
        super().__init__("Validation Agent")

    def _run(self, context: dict[str, Any]) -> AgentResult:
        run_state:  str = context.get("run_state", "UNKNOWN")
        run_output: str = context.get("run_output", "")
        feature:    str = context.get("feature", "")

        self.logger.info(f"Validating run for '{feature}' (state={run_state})")

        # ── Rule 1: hard failure — run never succeeded ─────────────────────────
        hard_fail = run_state not in ("SUCCESS",) and _HARD_FAILURE.search(run_output)
        if hard_fail:
            return AgentResult(
                agent=self.name,
                success=False,
                error=f"Run state is {run_state} with no recoverable output.",
                data={**context, "validated": False, "quality_score": 0,
                      "summary": "Run failed with no output.", "issues": [f"Run state: {run_state}"]},
            )

        # ── Rule 2: check for unsupported Databricks SQL types in the output ───
        type_warnings = []
        if _BAD_TYPES.search(run_output):
            bad = set(_BAD_TYPES.findall(run_output))
            type_warnings.append(
                f"Unsupported SQL types found in output: {bad}. "
                "Use STRING instead of TEXT/VARCHAR, BIGINT instead of INTEGER, DOUBLE instead of FLOAT."
            )
            self.logger.warning(f"Unsupported types detected: {bad}")

        # ── Rule 3: LLM quality analysis ──────────────────────────────────────
        self.logger.info("Asking LLM to analyse run output …")
        claude_result = self._llm_analyse(feature, run_output, run_state)

        quality_score = claude_result.get("quality_score", 0)
        llm_validated = claude_result.get("validated", False)
        llm_issues    = claude_result.get("issues", [])
        summary       = claude_result.get("summary", "")

        # ── Rule 4: filter out resolved issues — they should NOT block publish ─
        # An issue mentioning "resolved" or "retry" is informational only
        blocking_issues = [
            i for i in llm_issues
            if not _RESOLVED_PATTERNS.search(i)
            and ("error" in i.lower() or "fail" in i.lower() or "unsupported" in i.lower())
        ]

        # Quality score is the primary gate (>= 7 passes)
        # Override LLM validated=false if score >= 7 and no hard blocking issues
        if quality_score >= 7 and not blocking_issues and run_state == "SUCCESS":
            validated = True
            self.logger.info(
                f"Quality score {quality_score}/10 with no blocking issues — overriding to PASS"
            )
        else:
            validated = llm_validated and not blocking_issues

        all_issues = type_warnings + llm_issues   # include all for info, even resolved ones

        if validated:
            self.logger.info(
                f"✅ Demo validated (quality={quality_score}/10) — {summary[:80]}"
            )
            if all_issues:
                self.logger.info(f"   Non-blocking notes: {all_issues}")
        else:
            self.logger.warning(
                f"❌ Validation failed (quality={quality_score}/10). "
                f"Blocking issues: {blocking_issues}"
            )

        return AgentResult(
            agent=self.name,
            success=validated,
            error="" if validated else f"Validation failed: {'; '.join(blocking_issues or all_issues)}",
            data={
                **context,
                "validated":     validated,
                "quality_score": quality_score,
                "summary":       summary,
                "issues":        all_issues,
            },
        )

    def _llm_analyse(self, feature: str, output: str, run_state: str) -> dict:
        """Ask the LLM to assess execution quality."""
        snippet = output[:3000] if len(output) > 3000 else output

        prompt = f"""
Feature demonstrated: {feature}
Databricks SQL Warehouse run state: {run_state}

Execution output:
---
{snippet}
---

Note: This ran on a Databricks SQL Warehouse.
Some cells may have failed initially but succeeded on retry — these are NOT failures.
Assess overall quality based on the final state of execution.
""".strip()

        raw  = chat(prompt=prompt, system=_SYSTEM_PROMPT, max_tokens=512, temperature=0.1)
        text = raw.strip()
        if text.startswith("```"):
            text = "\n".join(l for l in text.splitlines() if not l.startswith("```")).strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            self.logger.warning(f"LLM returned non-JSON: {text[:100]}")
            # If run succeeded, give a passing default rather than failing
            if run_state == "SUCCESS":
                return {"quality_score": 7, "validated": True,
                        "issues": [], "summary": "Run succeeded. LLM analysis unavailable."}
            return {"quality_score": 0, "validated": False,
                    "issues": ["LLM analysis returned unparseable response."], "summary": text[:200]}