"""
agents/publisher_agent.py — Publisher Agent

Responsibility:
    Push the validated demo project to GitHub using the GitHub REST API.
    Commits notebook.py, queries.sql, and README.md under:
        projects/<date>-<slug>/

    Also appends a summary entry to the repo-level PROJECTS.md index.

Output data keys:
    github_url    — URL to the published project folder on GitHub
    commit_sha    — Git commit SHA of the publish commit
    commit_url    — Direct link to the commit
"""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any

import requests

from agents.base_agent import BaseAgent, AgentResult
from config import cfg


class PublisherAgent(BaseAgent):
    """Publishes a validated Databricks demo to GitHub."""

    def __init__(self):
        super().__init__("Publisher Agent")
        self._api = "https://api.github.com"
        self._headers = {
            "Authorization": f"Bearer {cfg.github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    # ── Public interface ───────────────────────────────────────────────────────

    def _run(self, context: dict[str, Any]) -> AgentResult:
        if not cfg.github_token:
            return AgentResult(
                agent=self.name,
                success=False,
                error="GITHUB_TOKEN is not set. Cannot publish to GitHub.",
            )

        project_dir   = Path(context.get("project_dir", ""))
        feature       = context.get("feature", "demo")
        slug          = context.get("slug", "demo")
        date_str      = context.get("date_str", "")
        project_idea  = context.get("project_idea", "")
        quality_score = context.get("quality_score", 0)
        summary       = context.get("summary", "")
        run_url       = context.get("run_url", "")

        if not project_dir.exists():
            return AgentResult(
                agent=self.name,
                success=False,
                error=f"Project directory does not exist: {project_dir}",
            )

        folder = f"projects/{date_str}-{slug}"
        commit_message = f"🤖 Add demo: {feature} ({date_str})"
        commit_shas: list[str] = []

        # ── Upload each file ───────────────────────────────────────────────────
        for filename in ["notebook.py", "queries.sql", "README.md"]:
            local_path = project_dir / filename
            if not local_path.exists():
                self.logger.warning(f"  Missing file, skipping: {filename}")
                continue

            gh_path = f"{folder}/{filename}"
            self.logger.info(f"  Uploading → {gh_path}")

            sha = self._get_file_sha(gh_path)  # needed to overwrite existing file
            content = base64.b64encode(local_path.read_bytes()).decode()

            resp = self._put_file(
                path=gh_path,
                message=commit_message,
                content=content,
                sha=sha,
            )
            commit_shas.append(resp["commit"]["sha"])
            time.sleep(0.5)  # Avoid secondary rate limits

        if not commit_shas:
            return AgentResult(
                agent=self.name,
                success=False,
                error="No files were uploaded to GitHub.",
            )

        # ── Update PROJECTS.md index ───────────────────────────────────────────
        self.logger.info("Updating PROJECTS.md index …")
        self._update_projects_index(
            folder=folder,
            feature=feature,
            date_str=date_str,
            project_idea=project_idea,
            quality_score=quality_score,
            summary=summary,
            run_url=run_url,
        )

        # ── Publish index.html (GitHub Pages portal) ───────────────────────────
        index_local = Path("index.html")
        if index_local.exists():
            self.logger.info("Publishing index.html to GitHub Pages …")
            try:
                sha = self._get_file_sha("index.html")
                html_content = base64.b64encode(index_local.read_bytes()).decode()
                self._put_file(
                    path="index.html",
                    message=f"[SPARK] Update portal: {feature} ({date_str})",
                    content=html_content,
                    sha=sha,
                )
                self.logger.info("  index.html published successfully")
            except Exception as e:
                self.logger.warning(f"  index.html publish failed (non-fatal): {e}")
        else:
            self.logger.warning("  index.html not found locally — skipping portal update")

        commit_sha = commit_shas[-1]
        commit_url = (
            f"https://github.com/{cfg.github_repo}/commit/{commit_sha}"
        )
        github_url = (
            f"https://github.com/{cfg.github_repo}/tree/{cfg.github_branch}/{folder}"
        )

        self.logger.info(f"Published → {github_url}")
        self.logger.info(f"Commit    → {commit_url}")

        return AgentResult(
            agent=self.name,
            success=True,
            data={
                **context,
                "github_url":  github_url,
                "commit_sha":  commit_sha,
                "commit_url":  commit_url,
            },
        )

    # ── GitHub API helpers ─────────────────────────────────────────────────────

    def _get_file_sha(self, path: str) -> str | None:
        """Return the blob SHA of a file if it already exists (needed for updates)."""
        url = f"{self._api}/repos/{cfg.github_repo}/contents/{path}"
        resp = requests.get(url, headers=self._headers, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("sha")
        return None

    def _put_file(
        self,
        path: str,
        message: str,
        content: str,
        sha: str | None,
    ) -> dict:
        url = f"{self._api}/repos/{cfg.github_repo}/contents/{path}"
        payload: dict = {
            "message": message,
            "content": content,
            "branch":  cfg.github_branch,
        }
        if sha:
            payload["sha"] = sha

        resp = requests.put(url, headers=self._headers, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _update_projects_index(
        self,
        folder: str,
        feature: str,
        date_str: str,
        project_idea: str,
        quality_score: int,
        summary: str,
        run_url: str,
    ) -> None:
        """Append a row to PROJECTS.md in the repo root."""
        index_path = "PROJECTS.md"
        sha = self._get_file_sha(index_path)

        # Read existing content or create header
        if sha:
            url = f"{self._api}/repos/{cfg.github_repo}/contents/{index_path}"
            raw = requests.get(url, headers=self._headers, timeout=15).json()
            existing = base64.b64decode(raw["content"]).decode("utf-8")
        else:
            existing = (
                "# 🤖 Databricks Daily AI Engineer — Project Index\n\n"
                "| Date | Feature | Demo | Quality | Run |\n"
                "|------|---------|------|---------|-----|\n"
            )

        run_link = f"[view run]({run_url})" if run_url else "N/A"
        new_row = (
            f"| {date_str} | **{feature}** | {project_idea} "
            f"| {quality_score}/10 | {run_link} |\n"
        )
        updated = existing + new_row

        self._put_file(
            path=index_path,
            message=f"📋 Update index: {feature} ({date_str})",
            content=base64.b64encode(updated.encode()).decode(),
            sha=sha,
        )