"""
agents/project_generator.py — Project Generator Agent

Responsibility:
    Use Claude to generate a complete, self-contained Databricks project for
    the selected feature. Writes three files to disk:
        projects/<date>-<feature>/notebook.py
        projects/<date>-<feature>/queries.sql
        projects/<date>-<feature>/README.md

Output data keys:
    project_dir   — absolute path to the generated project folder
    notebook_path — path to notebook.py
    sql_path      — path to queries.sql
    readme_path   — path to README.md
    feature       — feature name (passed through)
    project_idea  — project idea (passed through)
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any

from agents.base_agent import BaseAgent, AgentResult
from config import cfg
from utils.claude_client import chat


_NOTEBOOK_SYSTEM = """
You are an expert Databricks notebook developer. Generate production-quality
Python notebooks that:
- Use Databricks-native APIs (spark, dbutils, Delta Lake, etc.)
- Include clear section comments and docstrings
- Are fully self-contained (create any tables/data they need)
- Include a final validation cell that prints SUCCESS or FAILURE
- Are formatted as a valid Databricks .py notebook
  (each cell separated by # COMMAND ----------)

Respond ONLY with the notebook code. No markdown fences.
""".strip()

_SQL_SYSTEM = """
You are an expert Databricks SQL developer. Generate 3-5 meaningful SQL queries
that complement the notebook and demonstrate the feature from an analytics angle.
Each query must have a comment explaining what it does.
Respond ONLY with the SQL. No markdown fences.
""".strip()

_README_SYSTEM = """
You are a technical writer specialising in Databricks. Write a clear, concise
README.md that explains the demo project: what feature it demonstrates,
why it matters, how to run it, and what the expected output is.
Use proper markdown formatting.
""".strip()


class ProjectGeneratorAgent(BaseAgent):
    """Generates a full Databricks demo project using Claude."""

    def __init__(self):
        super().__init__("Project Generator")

    # ── Public interface ───────────────────────────────────────────────────────

    def _run(self, context: dict[str, Any]) -> AgentResult:
        feature = context.get("feature", "")
        description = context.get("description", "")
        project_idea = context.get("project_idea", "")
        tags = context.get("tags", [])

        if not feature:
            return AgentResult(
                agent=self.name,
                success=False,
                error="No feature provided by Feature Analyzer.",
            )

        # Create project directory
        date_str = datetime.now().strftime("%Y%m%d")
        slug = re.sub(r"[^a-z0-9]+", "-", feature.lower()).strip("-")
        project_dir = Path(cfg.projects_dir) / f"{date_str}-{slug}"
        project_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Project directory: {project_dir}")

        ctx = {
            "feature": feature,
            "description": description,
            "project_idea": project_idea,
            "tags": tags,
        }

        # Generate notebook
        self.logger.info("Generating notebook.py …")
        notebook_code = self._generate_notebook(ctx)
        notebook_path = project_dir / "notebook.py"
        notebook_path.write_text(notebook_code, encoding='utf-8')
        self.logger.info(f"  Written → {notebook_path} ({len(notebook_code)} chars)")

        # Generate SQL
        self.logger.info("Generating queries.sql …")
        sql_code = self._generate_sql(ctx)
        sql_path = project_dir / "queries.sql"
        sql_path.write_text(sql_code, encoding='utf-8')
        self.logger.info(f"  Written → {sql_path} ({len(sql_code)} chars)")

        # Generate README
        self.logger.info("Generating README.md …")
        readme = self._generate_readme(ctx)
        readme_path = project_dir / "README.md"
        readme_path.write_text(readme, encoding='utf-8')
        self.logger.info(f"  Written → {readme_path} ({len(readme)} chars)")

        return AgentResult(
            agent=self.name,
            success=True,
            data={
                "project_dir":   str(project_dir),
                "notebook_path": str(notebook_path),
                "sql_path":      str(sql_path),
                "readme_path":   str(readme_path),
                "feature":       feature,
                "project_idea":  project_idea,
                "slug":          slug,
                "date_str":      date_str,
            },
        )

    # ── Generation helpers ─────────────────────────────────────────────────────

    def _generate_notebook(self, ctx: dict) -> str:
        # Detect if we are targeting a SQL Warehouse — if so, generate SQL-first notebook
        from config import cfg
        sql_warehouse_mode = bool(cfg.databricks_http_path)

        if sql_warehouse_mode:
            prompt = f"""
Generate a Databricks notebook (.py format) targeting a SQL Warehouse.
IMPORTANT: SQL Warehouses can only execute SQL cells.
Every cell MUST be valid Spark SQL / Databricks SQL — NO Python code.

Feature:     {ctx['feature']}
Description: {ctx['description']}
Project:     {ctx['project_idea']}
Tags:        {', '.join(ctx['tags'])}

Schema & naming convention:
- Parent schema : daily_projects  (already exists, permissions granted)
- Project schema: daily_projects.{ctx.get('date_str','')}_{ctx.get('slug','demo').replace('-','_')}  (YYYYMMDD + underscore + slug)
- All tables    : daily_projects.{ctx.get('date_str','')}_{ctx.get('slug','demo').replace('-','_')}.<table_name>

For THIS project:
  date_str = {ctx.get('date_str','')}
  slug     = {ctx.get('slug','demo').replace('-','_')}
  schema   = daily_projects.{ctx.get('date_str','')}_{ctx.get('slug','demo').replace('-','_')}

Required cells in order (# COMMAND ---------- between every cell):

Cell 1 — Create project schema (YYYYMMDD_slug format):
  CREATE SCHEMA IF NOT EXISTS daily_projects.{ctx.get('date_str','')}_{ctx.get('slug','demo').replace('-','_')}
  COMMENT 'Demo: {ctx["feature"]} — {ctx["project_idea"]}'

Cell 2 — Set default schema context:
  USE daily_projects.{ctx.get('date_str','')}_{ctx.get('slug','demo').replace('-','_')}

Cell 3 — Create table (column definitions ONLY, no data in this cell):
  CREATE OR REPLACE TABLE daily_projects.{ctx.get('date_str','')}_{ctx.get('slug','demo').replace('-','_')}.<table_name> (
    col1 TYPE COMMENT 'description',
    col2 TYPE COMMENT 'description'
  )
  USING DELTA
  COMMENT 'table description'

Cell 4 — Insert sample data (ALWAYS a SEPARATE statement from CREATE):
  INSERT INTO daily_projects.{ctx.get('date_str','')}_{ctx.get('slug','demo').replace('-','_')}.<table_name> VALUES
  (val1, val2),
  (val3, val4)

Cell 5 — Core feature demonstration SQL relevant to {ctx['feature']}:
  (window functions / MERGE / OPTIMIZE / TIME TRAVEL / aggregations as appropriate)
  Always use full 3-part name: daily_projects.{ctx.get('date_str','')}_{ctx.get('slug','demo').replace('-','_')}.<table>

Cell 6 — Analytics insight SELECT query

Cell 7 — Final validation:
  SELECT
    '{ctx["feature"]}' AS feature_demonstrated,
    '{ctx.get('date_str','')}_{ctx.get('slug','demo').replace('-','_')}' AS schema_name,
    COUNT(*) AS rows_loaded,
    'SUCCESS' AS status
  FROM daily_projects.{ctx.get('date_str','')}_{ctx.get('slug','demo').replace('-','_')}.<table_name>

CRITICAL SQL Warehouse rules (violations cause runtime errors):
- INVALID: CREATE OR REPLACE TABLE t (col INT) AS VALUES (1)  <- never do this
- VALID  : CREATE OR REPLACE TABLE t (col INT)  then separately  INSERT INTO t VALUES (1)
- VALID  : CREATE OR REPLACE TABLE t AS SELECT ...  (no column list before AS SELECT)
- Always use full 3-part table names: daily_projects.<slug>.<table>
- NO Python, NO imports - pure SQL only
- Add -- SQL comments to explain each section

Start the file with:
# Databricks notebook source
# Feature: {ctx['feature']}
# Project: {ctx['project_idea']}
""".strip()
        else:
            prompt = f"""
Generate a complete Databricks notebook (.py format) that demonstrates:

Feature:     {ctx['feature']}
Description: {ctx['description']}
Project:     {ctx['project_idea']}
Tags:        {', '.join(ctx['tags'])}

Requirements:
1. First cell: imports and configuration (spark session is pre-existing)
2. Second cell: create or load required data / tables
3. Third cell: core feature demonstration code
4. Fourth cell: analytics / transformations on the result
5. Final cell: validation — print "Demo completed successfully" on success

Use # COMMAND ---------- to separate cells.
Start the file with:
# Databricks notebook source
# Feature: {ctx['feature']}
# Project: {ctx['project_idea']}
""".strip()

        return chat(prompt, system=_NOTEBOOK_SYSTEM, max_tokens=4096, temperature=0.2)

    def _generate_sql(self, ctx: dict) -> str:
        prompt = f"""
Generate 3-5 SQL queries for a Databricks demo that demonstrates:

Feature:     {ctx['feature']}
Project:     {ctx['project_idea']}

Each query must:
- Have a comment block explaining its purpose
- Work in Databricks SQL / Spark SQL
- Reference tables that the companion notebook would create
""".strip()

        return chat(prompt, system=_SQL_SYSTEM, max_tokens=2048, temperature=0.2)

    def _generate_readme(self, ctx: dict) -> str:
        prompt = f"""
Write a README.md for the following Databricks demo project:

Feature:     {ctx['feature']}
Description: {ctx['description']}
Project:     {ctx['project_idea']}
Tags:        {', '.join(ctx['tags'])}

Include:
- Overview section
- Why this feature matters
- Prerequisites
- How to run (notebook + SQL)
- Expected output
- Links to Databricks documentation
""".strip()

        return chat(prompt, system=_README_SYSTEM, max_tokens=2048, temperature=0.3)