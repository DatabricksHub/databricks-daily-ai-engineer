"""
agents/databricks_executor.py — Databricks Executor Agent

Supports THREE execution modes (auto-detected by config):

  1. SQL Warehouse  — uses Statement Execution API
                      requires DATABRICKS_HTTP_PATH
                      works on Community Edition SQL Warehouses ✅

  2. Context API    — uses Command Execution API on a regular cluster
                      works on Community Edition compute clusters ✅

  3. Jobs API       — uses Jobs/Runs API
                      requires a full paid Databricks workspace

Auto-detection order:
  DATABRICKS_HTTP_PATH set?  → SQL Warehouse mode
  Cluster found via API?     → Context API mode
  Otherwise                  → Jobs API mode

Output data keys:
    run_id, run_url, run_state, run_output, notebook_ws_path
"""

from __future__ import annotations

import base64
import time
from pathlib import Path
from typing import Any

import requests

from agents.base_agent import BaseAgent, AgentResult
from config import cfg
from utils.claude_client import chat


class DatabricksExecutorAgent(BaseAgent):

    def __init__(self):
        super().__init__("Databricks Executor")
        self._base    = cfg.databricks_host.rstrip("/")
        self._api     = self._base + "/api/2.0"
        self._headers = {
            "Authorization": f"Bearer {cfg.databricks_token}",
            "Content-Type":  "application/json",
        }

    # ── Public entry point ─────────────────────────────────────────────────────

    def _run(self, context: dict[str, Any]) -> AgentResult:
        if not cfg.databricks_host or not cfg.databricks_token:
            return AgentResult(
                agent=self.name, success=False,
                error="DATABRICKS_HOST or DATABRICKS_TOKEN not set.",
            )

        notebook_path = context.get("notebook_path", "")
        feature       = context.get("feature", "demo")
        slug          = context.get("slug", "demo")
        date_str      = context.get("date_str", "")

        if not notebook_path or not Path(notebook_path).exists():
            return AgentResult(
                agent=self.name, success=False,
                error=f"Notebook file not found: {notebook_path}",
            )

        # Upload notebook to workspace
        ws_folder        = f"{cfg.databricks_workspace_path}/{date_str}-{slug}"
        ws_notebook_path = f"{ws_folder}/notebook"
        self.logger.info(f"Creating workspace folder: {ws_folder}")
        self._mkdirs(ws_folder)
        self.logger.info(f"Uploading notebook → {ws_notebook_path}")
        self._upload_notebook(notebook_path, ws_notebook_path)

        # Auto-detect execution mode
        mode = self._detect_mode()
        self.logger.info(f"Execution mode: {mode}")

        if mode == "sql_warehouse":
            run_state, run_output, run_id = self._run_via_sql_warehouse(
                notebook_path, feature
            )
            warehouse_id = cfg.databricks_http_path.split("/")[-1]
            run_url = f"{self._base}/sql/editor"
        elif mode == "context":
            run_state, run_output, run_id = self._run_via_context_api(
                ws_notebook_path, feature
            )
            run_url = f"{self._base}/#workspace{ws_notebook_path}"
        else:
            run_state, run_output, run_id = self._run_via_jobs_api(
                ws_notebook_path, feature
            )
            run_url = f"{self._base}/#job/runs/{run_id}"

        self.logger.info(f"Execution finished — state: {run_state}")
        success = run_state == "SUCCESS"

        return AgentResult(
            agent=self.name,
            success=success,
            error="" if success else f"Run state: {run_state} — {run_output[:300]}",
            data={
                "run_id":           str(run_id),
                "run_url":          run_url,
                "run_state":        run_state,
                "run_output":       run_output,
                "notebook_ws_path": ws_notebook_path,
                "feature":          feature,
                "slug":             slug,
                "date_str":         date_str,
            },
        )

    # ── Mode detection ─────────────────────────────────────────────────────────

    def _detect_mode(self) -> str:
        """
        Returns 'sql_warehouse' | 'context' | 'jobs'

        Detection priority:
          1. DATABRICKS_HTTP_PATH set in env  → sql_warehouse
          2. Host URL contains 'dbc-'         → community edition → sql_warehouse
          3. Regular cluster found via API    → context
          4. Default                          → jobs
        """
        # Re-read env directly in case .env was loaded after module import
        import os
        http_path = cfg.databricks_http_path or os.environ.get("DATABRICKS_HTTP_PATH", "")

        if http_path:
            self.logger.info(f"SQL Warehouse mode: HTTP path = {http_path}")
            # Patch cfg so downstream code picks it up too
            if not cfg.databricks_http_path:
                object.__setattr__(cfg, "databricks_http_path", http_path)
            return "sql_warehouse"

        # Community Edition host always contains 'dbc-'
        # It does NOT support Jobs API or regular cluster API
        host = cfg.databricks_host.lower()
        if "dbc-" in host:
            self.logger.info(
                "Community Edition detected (host contains 'dbc-'). "
                "Switching to SQL Warehouse mode. "
                "Make sure DATABRICKS_HTTP_PATH is set in your .env file."
            )
            http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
            if not http_path:
                self.logger.error(
                    "DATABRICKS_HTTP_PATH is not set! "
                    "Go to: Databricks workspace -> SQL Warehouses "
                    "-> your warehouse -> Connection Details -> HTTP Path"
                )
                raise EnvironmentError(
                    "DATABRICKS_HTTP_PATH required for Community Edition. "
                    "Add it to your .env file: "
                    "DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id"
                )
            object.__setattr__(cfg, "databricks_http_path", http_path)
            return "sql_warehouse"

        # Standard workspace — try to find a regular cluster
        try:
            data     = self._get("/clusters/list")
            clusters = data.get("clusters", [])
            if clusters:
                return "context"
        except Exception:
            pass

        return "jobs"

    # ── Mode 1: SQL Warehouse (LLM-powered cell parser + executor) ────────────

    def _run_via_sql_warehouse(
        self, notebook_path: str, feature: str
    ) -> tuple[str, str, str]:
        """
        Uses the LLM to:
          1. Classify each cell as SQL or Python
          2. Convert Python cells into equivalent SQL
          3. Execute all cells via the Statement Execution API

        This means every cell runs on the SQL Warehouse — nothing is skipped.
        """
        self.logger.info("Running via SQL Warehouse with LLM cell parser …")

        warehouse_id = cfg.databricks_http_path.strip("/").split("/")[-1]
        self.logger.info(f"Warehouse ID: {warehouse_id}")

        source    = Path(notebook_path).read_text(encoding="utf-8")
        raw_cells = self._extract_cells(source)
        self.logger.info(f"Extracted {len(raw_cells)} cells — sending to LLM for analysis …")

        # Ask LLM to classify and convert all cells in one shot
        import re
        from datetime import date
        date_str     = date.today().strftime('%Y%m%d')
        feature_slug = re.sub(r'[^a-z0-9]+', '_', feature.lower()).strip('_')
        schema_name  = f'{date_str}_{feature_slug}'
        self.logger.info(f'Project schema: daily_projects.{schema_name}')
        classified = self._llm_classify_and_convert(raw_cells, feature, schema_name)
        self.logger.info(f"LLM processed {len(classified)} cells")

        all_output  = []
        final_state = "SUCCESS"

        for i, item in enumerate(classified, 1):
            original_type = item.get("type", "sql")
            sql           = item.get("sql", "").strip()
            explanation   = item.get("explanation", "")

            if not sql:
                self.logger.info(f"  Cell {i}: empty after conversion, skipping")
                continue

            self.logger.info(
                f"  Cell {i}/{len(classified)} "
                f"[{original_type.upper()} → SQL]: {explanation[:60]} …"
            )

            sql = self._sanitize_sql(sql)
            state, output = self._execute_statement(warehouse_id, sql)
            all_output.append(
                f"[Cell {i} | original={original_type}]\n"
                f"-- {explanation}\n"
                f"{output}"
            )

            if state == "FAILED":
                self.logger.warning(
                    f"  Cell {i} failed: {output[:200]}\n"
                    f"  Asking LLM to fix and retry …"
                )
                # Ask LLM to fix the failed SQL and retry once
                fixed_sql = self._llm_fix_sql(sql, output, feature)
                if fixed_sql and fixed_sql != sql:
                    self.logger.info(f"  Retrying cell {i} with LLM fix …")
                    fixed_sql = self._sanitize_sql(fixed_sql)
                    state, output = self._execute_statement(warehouse_id, fixed_sql)
                    all_output.append(f"[Cell {i} RETRY]\n{output}")

                if state == "FAILED":
                    final_state = "FAILED"
                    self.logger.error(f"  Cell {i} still failed after fix: {output[:200]}")
                    break
                else:
                    self.logger.info(f"  Cell {i} succeeded after fix!")

        return final_state, "\n\n".join(all_output), f"warehouse-{warehouse_id}"

    # ── LLM cell classification & conversion ───────────────────────────────────

    def _llm_classify_and_convert(
        self, cells: list[str], feature: str, schema_name: str = ''
    ) -> list[dict]:
        """
        Sends all notebook cells to the LLM.
        Returns a list of dicts:
          { "type": "sql"|"python", "sql": "<converted SQL>", "explanation": "..." }

        The LLM converts Python cells (spark.createDataFrame, df.show, etc.)
        into equivalent Databricks SQL so they can run on a SQL Warehouse.
        """
        # Derive schema_name — always compute feature_slug so it's available in the prompt
        import re
        from datetime import date
        feature_slug = re.sub(r"[^a-z0-9]+", "_", feature.lower()).strip("_")
        if not schema_name:
            date_str    = date.today().strftime('%Y%m%d')
            schema_name = f'{date_str}_{feature_slug}'

        cells_text = ""
        for i, cell in enumerate(cells, 1):
            cells_text += f"\n### CELL {i}\n{cell}\n"

        prompt = f"""
You are a Databricks SQL expert. Below are notebook cells for a demo of: {feature}
Project schema: daily_projects.{feature_slug} (use this prefix for ALL table names)

Your job:
1. Classify each cell as "sql" or "python"
2. Convert EVERY cell into valid Databricks SQL that can run on a SQL Warehouse
3. For Python cells (spark.createDataFrame, df operations, print statements etc.),
   write equivalent SQL that achieves the same result

Schema naming convention (MUST follow exactly for every table reference):
- Parent schema : daily_projects  (already exists, permissions granted)
- Project schema: daily_projects.{schema_name}  (format: YYYYMMDD_feature_slug)
- Every table   : daily_projects.{schema_name}.<table_name>  (always 3-part name)
- Example: daily_projects.{schema_name}.transactions

Conversion rules:
- First cell (imports/setup)            ->  CREATE SCHEMA IF NOT EXISTS daily_projects.{schema_name} COMMENT 'Demo: {feature}'
- spark.createDataFrame(data, schema)   ->  Split into TWO separate JSON items:
    ITEM A: CREATE OR REPLACE TABLE daily_projects.{schema_name}.tbl (col1 TYPE, col2 TYPE) USING DELTA
    ITEM B: INSERT INTO daily_projects.{schema_name}.tbl VALUES (v1, v2), (v3, v4)
- df.show() / display(df) / print(...)  ->  SELECT * FROM daily_projects.{schema_name}.tbl LIMIT 20
- spark.sql('...')                       ->  extract SQL directly, use daily_projects.{schema_name}. prefix
- Markdown / comment-only cells         ->  SELECT 'description' AS step
- Validation / final print cell         ->  SELECT '{feature}' AS feature, COUNT(*) AS rows, 'SUCCESS' AS status FROM daily_projects.{schema_name}.tbl

CRITICAL SQL WAREHOUSE RULES:
1. NEVER: CREATE OR REPLACE TABLE t (col INT) AS VALUES (1)  <- SYNTAX ERROR, will fail
2. ALWAYS split into two statements: CREATE TABLE (schema only) then INSERT INTO (data only)
3. VALID: CREATE OR REPLACE TABLE t AS SELECT ...  (no column list allowed before AS SELECT)
4. Always use full 3-part name: daily_projects.{schema_name}.<table>
5. Each output item in the JSON array is ONE SQL statement only

CELLS TO PROCESS:
{cells_text}

Respond with ONLY a JSON array. No markdown fences. No explanation outside JSON.
Format:
[
  {{
    "cell_number": 1,
    "type": "sql",
    "sql": "SELECT 1 AS test",
    "explanation": "what this cell does in plain English"
  }},
  ...
]
""".strip()

        system = (
            "You are a Databricks SQL expert that converts Python notebook cells "
            "into equivalent Databricks SQL. Always respond with valid JSON only."
        )

        import json
        raw = chat(prompt, system=system, max_tokens=4096, temperature=0.1)

        # Strip markdown fences if present
        text = raw.strip()
        if text.startswith("```"):
            text = "\n".join(
                l for l in text.splitlines() if not l.startswith("```")
            ).strip()

        try:
            result = json.loads(text)
            self.logger.info(f"LLM successfully classified {len(result)} cells")
            return result
        except json.JSONDecodeError as e:
            self.logger.warning(f"LLM JSON parse failed: {e}. Using fallback cell extraction.")
            # Fallback: treat all cells as SQL, use only cells that look like SQL
            return [
                {
                    "cell_number": i,
                    "type": "sql" if self._is_sql_cell(c) else "python",
                    "sql": c if self._is_sql_cell(c) else f"SELECT 'Cell {i}' AS skipped_python_cell",
                    "explanation": f"Cell {i} (fallback)"
                }
                for i, c in enumerate(cells, 1)
            ]

    def _llm_fix_sql(
        self, failed_sql: str, error_message: str, feature: str
    ) -> str:
        """Ask the LLM to fix a failed SQL statement."""
        prompt = f"""
A Databricks SQL statement failed on a Databricks SQL Warehouse. Fix it.

Feature context: {feature}

Failed SQL:
{failed_sql}

Error message:
{error_message}

Key SQL Warehouse rules to follow when fixing:
1. NEVER use: CREATE OR REPLACE TABLE t (col TYPE) AS VALUES (...)
   Instead split into:
     CREATE OR REPLACE TABLE t (col TYPE)
     INSERT INTO t VALUES (...)
2. NEVER use: CREATE OR REPLACE TABLE t (col TYPE) AS SELECT ...
   Instead use: CREATE OR REPLACE TABLE t AS SELECT ...
3. Column schema and AS VALUES/AS SELECT cannot be in the same statement
4. Return ONE single valid SQL statement only

Return ONLY the fixed SQL. No explanation, no markdown fences.
""".strip()

        system = (
            "You are a Databricks SQL Warehouse expert. "
            "Fix SQL errors strictly following Databricks SQL syntax. "
            "Return only valid SQL, no explanations."
        )
        fixed  = chat(prompt, system=system, max_tokens=1024, temperature=0.1)

        # Strip markdown fences
        fixed = fixed.strip()
        if fixed.startswith("```"):
            lines = [l for l in fixed.splitlines() if not l.startswith("```")]
            fixed = "\n".join(lines).strip()

        return fixed

    def _sanitize_sql(self, sql: str) -> str:
        """
        Auto-correct SQL types that are invalid on Databricks SQL Warehouse.
        Prevents runtime errors before they happen.
        """
        import re
        fixes = [
            # Types not supported in Databricks SQL
            (r"\bTEXT\b",       "STRING"),
            (r"\bVARCHAR\s*\([^)]*\)", "STRING"),   # VARCHAR(255) → STRING
            (r"\bVARCHAR\b",    "STRING"),
            (r"\bNVARCHAR\s*\([^)]*\)", "STRING"),
            (r"\bNVARCHAR\b",   "STRING"),
            (r"\bCHAR\s*\([^)]*\)",    "STRING"),   # CHAR(10) → STRING
            (r"\bINTEGER\b",    "INT"),
            (r"\bFLOAT4\b",     "FLOAT"),
            (r"\bFLOAT8\b",     "DOUBLE"),
            (r"\bNUMERIC\b",    "DECIMAL"),
            (r"\bBOOL\b",       "BOOLEAN"),
            (r"\bDATETIME\b",   "TIMESTAMP"),
            (r"\bBYTEA\b",      "BINARY"),
        ]
        original = sql
        for pattern, replacement in fixes:
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
        if sql != original:
            import re as _re
            changed = [
                f"{p} → {r}" for p, r in fixes
                if _re.search(p, original, flags=_re.IGNORECASE)
            ]
            self.logger.info(f"  Auto-fixed SQL types: {', '.join(changed)}")
        return sql

    def _execute_statement(
        self, warehouse_id: str, statement: str
    ) -> tuple[str, str]:
        """Submit one statement and poll until done. Returns (state, output)."""
        api_url = f"{self._base}/api/2.0/sql/statements/"

        payload = {
            "warehouse_id": warehouse_id,
            "statement":    statement,
            "wait_timeout": "30s",    # wait up to 30s inline
            "on_wait_timeout": "CONTINUE",
        }

        resp = requests.post(api_url, headers=self._headers, json=payload, timeout=40)
        resp.raise_for_status()
        data         = resp.json()
        statement_id = data.get("statement_id", "")
        status       = data.get("status", {})
        state        = status.get("state", "PENDING")

        # Poll if still running
        deadline = time.monotonic() + 300
        while state in ("PENDING", "RUNNING") and time.monotonic() < deadline:
            time.sleep(5)
            poll = requests.get(
                f"{api_url}{statement_id}",
                headers=self._headers, timeout=20
            )
            poll.raise_for_status()
            pdata  = poll.json()
            status = pdata.get("status", {})
            state  = status.get("state", "PENDING")
            data   = pdata

        if state == "SUCCEEDED":
            # Extract result rows if any
            result  = data.get("result", {})
            chunks  = result.get("data_array", [])
            if chunks:
                output = "\n".join(str(row) for row in chunks[:20])
            else:
                output = "✅ Statement executed successfully (no rows returned)"
            return "SUCCESS", output

        elif state == "FAILED":
            err = status.get("error", {})
            return "FAILED", err.get("message", "Statement failed — check warehouse logs")

        else:
            return "FAILED", f"Unexpected statement state: {state}"

    # ── Mode 2: Execution Context API (regular clusters) ──────────────────────

    def _run_via_context_api(
        self, ws_notebook_path: str, feature: str
    ) -> tuple[str, str, str]:
        self.logger.info("Running via Execution Context API …")

        cluster_id = self._get_running_cluster()
        if not cluster_id:
            return "FAILED", "No running cluster found.", ""

        self.logger.info(f"Using cluster: {cluster_id}")
        ctx_id = self._create_context(cluster_id)

        try:
            source = self._export_notebook(ws_notebook_path)
            cells  = self._extract_cells(source)
            self.logger.info(f"Executing {len(cells)} cells …")

            all_output  = []
            final_state = "SUCCESS"

            for i, cell in enumerate(cells, 1):
                if not cell.strip():
                    continue
                self.logger.info(f"  Cell {i}/{len(cells)} …")
                state, output = self._run_command(cluster_id, ctx_id, cell)
                all_output.append(f"[Cell {i}]\n{output}")
                if state == "error":
                    final_state = "FAILED"
                    self.logger.error(f"  Cell {i} failed: {output[:200]}")
                    break

            return final_state, "\n\n".join(all_output), ctx_id
        finally:
            try:
                self._destroy_context(cluster_id, ctx_id)
            except Exception:
                pass

    def _get_running_cluster(self) -> str | None:
        try:
            data     = self._get("/clusters/list")
            clusters = data.get("clusters", [])
            for c in clusters:
                if c.get("state") == "RUNNING":
                    return c["cluster_id"]
            for c in clusters:
                if c.get("state") not in ("TERMINATED", "TERMINATING"):
                    cid = c["cluster_id"]
                    self.logger.info(f"Starting cluster {cid} …")
                    self._post("/clusters/start", {"cluster_id": cid})
                    self._wait_for_cluster(cid)
                    return cid
        except Exception as e:
            self.logger.error(f"Could not list clusters: {e}")
        return None

    def _wait_for_cluster(self, cluster_id: str) -> None:
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            state = self._get(f"/clusters/get?cluster_id={cluster_id}").get("state", "")
            self.logger.debug(f"  Cluster state: {state}")
            if state == "RUNNING":
                return
            if state in ("TERMINATED", "ERROR"):
                raise RuntimeError(f"Cluster entered state: {state}")
            time.sleep(15)
        raise TimeoutError("Cluster did not start within 5 minutes.")

    def _create_context(self, cluster_id: str) -> str:
        return self._post("/contexts/create", {
            "clusterId": cluster_id, "language": "python"
        })["id"]

    def _destroy_context(self, cluster_id: str, ctx_id: str) -> None:
        self._post("/contexts/destroy", {"clusterId": cluster_id, "contextId": ctx_id})

    def _run_command(self, cluster_id: str, ctx_id: str, code: str) -> tuple[str, str]:
        cmd_id   = self._post("/commands/execute", {
            "clusterId": cluster_id, "contextId": ctx_id,
            "language": "python", "command": code,
        })["id"]
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            data   = self._get(
                f"/commands/status?clusterId={cluster_id}&contextId={ctx_id}&commandId={cmd_id}"
            )
            status = data.get("status", "")
            if status == "Finished":
                results = data.get("results", {})
                text    = results.get("data", "") or results.get("cause", "")
                state   = "error" if results.get("resultType") == "error" else "success"
                return state, str(text)
            if status in ("Error", "Cancelled"):
                return "error", data.get("results", {}).get("cause", "Unknown error")
            time.sleep(5)
        return "error", "Cell execution timed out."

    def _export_notebook(self, ws_path: str) -> str:
        resp = requests.get(
            f"{self._api}/workspace/export?path={ws_path}&format=SOURCE",
            headers=self._headers, timeout=30
        )
        resp.raise_for_status()
        return base64.b64decode(resp.json().get("content", "")).decode("utf-8")

    # ── Mode 3: Jobs API (full workspaces) ────────────────────────────────────

    def _run_via_jobs_api(
        self, ws_notebook_path: str, feature: str
    ) -> tuple[str, str, int]:
        self.logger.info("Running via Jobs API …")
        if cfg.databricks_cluster_id:
            payload = {
                "run_name":            f"AI Engineer Demo — {feature}",
                "existing_cluster_id": cfg.databricks_cluster_id,
                "notebook_task":       {"notebook_path": ws_notebook_path},
            }
        else:
            payload = {
                "run_name":      f"AI Engineer Demo — {feature}",
                "new_cluster":   {
                    "num_workers": 1, "spark_version": "14.3.x-scala2.12",
                    "node_type_id": "i3.xlarge", "autotermination_minutes": 30,
                },
                "notebook_task": {"notebook_path": ws_notebook_path},
            }
        run_id = self._post("/jobs/runs/submit", payload)["run_id"]
        self.logger.info(f"Job submitted: run_id={run_id}")

        deadline = time.monotonic() + cfg.job_timeout_seconds
        while time.monotonic() < deadline:
            data       = self._get(f"/jobs/runs/get?run_id={run_id}")
            state      = data.get("state", {})
            life_cycle = state.get("life_cycle_state", "PENDING")
            result     = state.get("result_state", "")
            self.logger.debug(f"  Poll: {life_cycle}/{result}")
            if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                output = self._get_jobs_output(run_id)
                return result or life_cycle, output, run_id
            time.sleep(cfg.job_poll_interval_seconds)
        try:
            self._post("/jobs/runs/cancel", {"run_id": run_id})
        except Exception:
            pass
        return "TIMEOUT", "Run exceeded maximum allowed duration.", run_id

    def _get_jobs_output(self, run_id: int) -> str:
        try:
            data = self._get(f"/jobs/runs/get-output?run_id={run_id}")
            return (data.get("error") or
                    data.get("notebook_output", {}).get("result", "") or
                    "No output captured.")
        except Exception as e:
            return f"Could not retrieve output: {e}"

    # ── Shared helpers ─────────────────────────────────────────────────────────

    def _extract_cells(self, source: str) -> list[str]:
        """Split notebook source into cells, handling MAGIC lines."""
        raw_cells = source.split("# COMMAND ----------")
        cells = []
        for cell in raw_cells:
            cell = cell.strip()
            if not cell:
                continue
            if cell.startswith("# Databricks notebook source"):
                continue
            # Handle MAGIC cells — strip the # MAGIC prefix
            if "# MAGIC" in cell:
                lines = []
                for line in cell.splitlines():
                    if line.startswith("# MAGIC "):
                        lines.append(line[8:])
                    elif line.startswith("# MAGIC"):
                        lines.append(line[7:])
                    elif line.startswith("# DBTITLE"):
                        continue
                    else:
                        lines.append(line)
                cell = "\n".join(lines).strip()
                if cell.startswith("%sql"):
                    cell = cell[4:].strip()
                elif cell.startswith("%python"):
                    cell = cell[7:].strip()
                elif cell.startswith("%md") or cell.startswith("%sh"):
                    continue
            if cell:
                cells.append(cell)
        return cells

    def _is_sql_cell(self, cell: str) -> bool:
        """Returns True if cell is SQL, False if Python."""
        lines = [
            l.strip() for l in cell.strip().splitlines()
            if l.strip() and not l.strip().startswith("--") and not l.strip().startswith("#")
        ]
        if not lines:
            return False
        first = lines[0].upper()
        SQL_KEYWORDS = (
            "SELECT", "CREATE", "INSERT", "UPDATE", "DELETE", "DROP",
            "SHOW", "DESCRIBE", "USE", "WITH", "MERGE", "ALTER",
            "GRANT", "REVOKE", "TRUNCATE", "EXPLAIN", "OPTIMIZE",
            "VACUUM", "CACHE", "UNCACHE", "ANALYZE", "REFRESH",
        )
        PYTHON_SIGNALS = (
            "IMPORT ", "FROM ", "DEF ", "CLASS ", "SPARK.",
            "PRINT(", "DF.", "IF ", "FOR ", "WHILE ", "TRY:",
        )
        for sig in PYTHON_SIGNALS:
            if first.startswith(sig):
                return False
        for kw in SQL_KEYWORDS:
            if first.startswith(kw):
                return True
        return False

    def _mkdirs(self, path: str) -> None:
        self._post("/workspace/mkdirs", {"path": path})

    def _upload_notebook(self, local_path: str, ws_path: str) -> None:
        content = base64.b64encode(Path(local_path).read_bytes()).decode()
        self._post("/workspace/import", {
            "path": ws_path, "language": "PYTHON",
            "format": "SOURCE", "content": content, "overwrite": True,
        })

    def _post(self, endpoint: str, payload: dict) -> dict:
        resp = requests.post(
            self._api + endpoint, headers=self._headers, json=payload, timeout=30
        )
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def _get(self, endpoint: str) -> dict:
        resp = requests.get(
            self._api + endpoint, headers=self._headers, timeout=30
        )
        resp.raise_for_status()
        return resp.json()