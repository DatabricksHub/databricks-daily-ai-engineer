"""
agents/page_generator.py — Page Generator Agent

Generates and updates index.html — the DatabricksHub public knowledge portal.
Runs after every successful pipeline execution.

Agent name: SPARK
  Self-governing Platform for Automated Research and Knowledge
  A nod to Apache Spark, the engine at the heart of Databricks.
"""

from __future__ import annotations

import json
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any

from agents.base_agent import BaseAgent, AgentResult
from config import cfg


class PageGeneratorAgent(BaseAgent):

    def __init__(self):
        super().__init__("Page Generator")

    def _run(self, context: dict[str, Any]) -> AgentResult:
        history = self._load_history()

        # Append current run if validated
        if context.get("validated") and context.get("feature"):
            entry = {
                "date":          context.get("date_str", datetime.now().strftime("%Y%m%d")),
                "feature":       context.get("feature", ""),
                "project_idea":  context.get("project_idea", ""),
                "schema":        (
                    f"{cfg.databricks_parent_schema}"
                    f".{context.get('date_str','')}_{context.get('slug','').replace('-','_')}"
                ),
                "notebook_url":  context.get("run_url", ""),
                "github_url":    context.get("github_url", ""),
                "quality_score": context.get("quality_score", 0),
            }
            key = entry["date"] + entry["feature"]
            if key not in [h.get("date","") + h.get("feature","") for h in history]:
                history.insert(0, entry)
                self._save_history(history)

        html = self._render(history)
        out  = Path("index.html")
        out.write_text(html, encoding="utf-8")
        self.logger.info(f"Landing page written -> {out.resolve()}")

        return AgentResult(
            agent=self.name, success=True,
            data={**context, "page_path": str(out.resolve())},
        )

    # ── Persistence ────────────────────────────────────────────────────────────

    def _load_history(self) -> list[dict]:
        p = Path("page_history.json")
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                pass
        return []

    def _save_history(self, history: list[dict]) -> None:
        Path("page_history.json").write_text(
            json.dumps(history, indent=2), encoding="utf-8"
        )

    # ── Rendering ──────────────────────────────────────────────────────────────

    def _render(self, history: list[dict]) -> str:
        updated   = datetime.now().strftime("%B %d, %Y at %H:%M")
        proj_rows = self._project_rows(history)
        feed_html = self._feed_html()
        run_count = len(history)

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SPARK — DatabricksHub Knowledge Portal</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  :root {{
    --ink:    #0e0f11;
    --paper:  #f5f2eb;
    --accent: #c0392b;
    --gold:   #b5892a;
    --steel:  #2c3e50;
    --muted:  #6b6b6b;
    --rule:   #d4cfc4;
    --mono:   'IBM Plex Mono', monospace;
    --serif:  'Libre Baskerville', Georgia, serif;
    --sans:   'IBM Plex Sans', sans-serif;
    --wide:   1140px;
  }}
  html {{ scroll-behavior: smooth; }}
  body {{ background: var(--paper); color: var(--ink); font-family: var(--serif);
    font-size: 16px; line-height: 1.75; -webkit-font-smoothing: antialiased; }}

  /* Masthead */
  .masthead {{ background: var(--ink); border-bottom: 2px solid var(--accent); }}
  .masthead-inner {{ max-width: var(--wide); margin: 0 auto; padding: 14px 40px;
    display: flex; align-items: center; justify-content: space-between;
    gap: 20px; flex-wrap: wrap; }}
  .masthead-left {{ display: flex; align-items: center; gap: 20px; flex-wrap: wrap; }}
  .masthead-wordmark {{ font-family: var(--mono); font-size: 15px; letter-spacing: 0.2em;
    text-transform: uppercase; color: var(--paper); font-weight: 500; white-space: nowrap; }}
  .masthead-wordmark span {{ color: var(--accent); }}
  .masthead-divider {{ width: 1px; height: 18px; background: #333; }}
  .masthead-subtitle {{ font-family: var(--sans); font-size: 12px; color: #bbb; white-space: nowrap; }}
  .masthead-meta {{ font-family: var(--mono); font-size: 10px; color: #aaa;
    text-align: right; line-height: 1.7; white-space: nowrap; }}

  /* Nav */
  .nav {{ background: #1a1c1f; border-bottom: 1px solid #2a2c2f; overflow-x: auto; }}
  .nav-inner {{ max-width: var(--wide); margin: 0 auto; padding: 0 40px;
    display: flex; white-space: nowrap; }}
  .nav a {{ font-family: var(--mono); font-size: 10px; letter-spacing: 0.18em;
    text-transform: uppercase; color: #bbb; text-decoration: none;
    padding: 11px 18px; display: inline-block;
    border-bottom: 2px solid transparent; transition: color 0.15s, border-color 0.15s; }}
  .nav a:hover {{ color: var(--paper); border-color: var(--accent); }}

  /* Hero */
  .hero {{ max-width: var(--wide); margin: 0 auto; padding: 64px 40px 48px;
    display: grid; grid-template-columns: 1fr 300px; gap: 56px; align-items: start; }}
  .hero-eyebrow {{ font-family: var(--mono); font-size: 10px; letter-spacing: 0.22em;
    text-transform: uppercase; color: var(--accent); margin-bottom: 16px; }}
  .hero-title {{ font-size: clamp(36px, 5vw, 62px); font-weight: 700;
    line-height: 1.05; letter-spacing: -0.02em; margin-bottom: 6px; }}
  .hero-tagline {{ font-family: var(--mono); font-size: 12px; color: var(--gold);
    letter-spacing: 0.1em; margin-bottom: 24px; }}
  .hero-lead {{ font-size: 16px; line-height: 1.8; color: #3a3a3a; max-width: 580px; }}
  .hero-lead + .hero-lead {{ margin-top: 12px; }}
  .hero-lead em {{ font-style: italic; color: var(--steel); }}

  /* Status card */
  .status-card {{ background: var(--ink); color: var(--paper); padding: 26px;
    border-top: 3px solid var(--accent); font-family: var(--mono);
    font-size: 11px; line-height: 1.8; position: sticky; top: 20px; }}
  .status-card-title {{ font-size: 9px; letter-spacing: 0.25em; text-transform: uppercase;
    color: #aaa; margin-bottom: 14px; padding-bottom: 10px; border-bottom: 1px solid #333; }}
  .stat {{ display: flex; justify-content: space-between; padding: 5px 0;
    border-bottom: 1px solid #2a2a2a; color: #ccc; }}
  .stat:last-child {{ border-bottom: none; }}
  .stat-val {{ color: var(--gold); font-weight: 500; }}

  /* Sections */
  .section {{ max-width: var(--wide); margin: 0 auto; padding: 52px 40px 48px; }}
  .section-title {{ font-size: clamp(36px, 4vw, 58px); font-weight: 700;
    letter-spacing: -0.02em; margin-bottom: 20px; line-height: 1.05; text-align: center; }}
  .intro-grid .section-title {{ text-align: left; font-size: 22px;
    letter-spacing: -0.01em; margin-bottom: 14px; }}

  /* Agent intro */
  .intro-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 56px; align-items: start; }}
  .intro-body p {{ font-size: 15px; color: #3a3a3a; margin-bottom: 14px;
    line-height: 1.8; text-align: left; }}
  .intro-body p strong {{ color: var(--ink); }}
  .pull-quote {{ font-size: 17px; font-style: italic; line-height: 1.6; color: var(--steel);
    border-left: 3px solid var(--accent); padding: 10px 0 10px 22px;
    margin: 22px 0; text-align: left; }}

  /* Agent flow */
  .agent-flow {{ display: flex; flex-direction: column; }}
  .agent-node {{ display: grid; grid-template-columns: 40px 1fr; gap: 0; }}
  .agent-connector {{ display: flex; flex-direction: column; align-items: center; }}
  .agent-num {{ width: 40px; height: 40px; background: var(--steel); color: var(--paper);
    border-radius: 50%; display: flex; align-items: center; justify-content: center;
    font-family: var(--mono); font-size: 11px; font-weight: 500; flex-shrink: 0; z-index: 1; }}
  .agent-node:last-child .agent-num {{ background: var(--gold); color: var(--ink); }}
  .agent-line {{ width: 2px; flex: 1; background: var(--rule); min-height: 16px; }}
  .agent-node:last-child .agent-line {{ display: none; }}
  .agent-body {{ padding: 10px 0 22px 18px; }}
  .agent-name {{ font-family: var(--sans); font-size: 13px; font-weight: 600;
    color: var(--ink); margin-bottom: 3px; }}
  .agent-desc {{ font-family: var(--sans); font-size: 12px; color: var(--muted); line-height: 1.6; }}
  .agent-output {{ font-family: var(--mono); font-size: 10px; color: var(--gold); margin-top: 5px; }}

  /* Table */
  .table-wrap {{ overflow-x: auto; border: 1px solid var(--rule); margin-top: 16px; }}
  table {{ width: 100%; border-collapse: collapse; font-family: var(--sans); font-size: 13px; }}
  thead tr {{ background: var(--ink); color: var(--paper); }}
  thead th {{ padding: 11px 14px; text-align: left; font-family: var(--mono); font-size: 9px;
    letter-spacing: 0.18em; text-transform: uppercase; font-weight: 500; white-space: nowrap; }}
  tbody tr {{ border-bottom: 1px solid var(--rule); transition: background 0.12s; }}
  tbody tr:last-child {{ border-bottom: none; }}
  tbody tr:hover {{ background: #ede9df; }}
  td {{ padding: 11px 14px; vertical-align: middle; color: #2a2a2a; }}
  td.date-col    {{ font-family: var(--mono); font-size: 11px; color: #888; white-space: nowrap; }}
  td.feature-col {{ font-weight: 600; color: var(--ink); white-space: nowrap; }}
  td.project-col {{ font-size: 12px; color: #555; }}
  td.schema-col  {{ font-family: var(--mono); font-size: 10px; color: var(--steel); white-space: nowrap; }}
  .table-link {{ font-family: var(--mono); font-size: 10px; color: var(--accent);
    text-decoration: none; border-bottom: 1px solid transparent;
    transition: border-color 0.12s; white-space: nowrap; }}
  .table-link:hover {{ border-color: var(--accent); }}
  .badge {{ display: inline-block; background: #e8e4da; color: #555;
    font-family: var(--mono); font-size: 9px; padding: 2px 7px; }}

  /* Knowledge feeds */
  .feeds-cats {{ display: grid; grid-template-columns: repeat(4, 1fr);
    gap: 32px; margin-top: 32px; }}
  .feeds-cat-title {{ font-family: var(--sans); font-size: 10px; font-weight: 600;
    letter-spacing: 0.15em; text-transform: uppercase; color: var(--steel);
    margin-bottom: 12px; padding-bottom: 8px; border-bottom: 2px solid var(--rule); }}
  .feed-link {{ display: block; font-family: var(--sans); font-size: 12px;
    color: var(--steel); text-decoration: none; padding: 4px 0;
    border-bottom: 1px solid #ede9df; line-height: 1.45; transition: color 0.12s; }}
  .feed-link:last-child {{ border-bottom: none; }}
  .feed-link:hover {{ color: var(--accent); }}

  /* Docs */
  .docs-grid {{ display: grid; grid-template-columns: repeat(3, 1fr);
    gap: 1px; background: var(--rule); border: 1px solid var(--rule); margin-top: 24px; }}
  .doc-card {{ background: var(--paper); padding: 20px 22px; transition: background 0.12s; }}
  .doc-card:hover {{ background: #ede9df; }}
  .doc-cat {{ font-family: var(--sans); font-size: 9px; font-weight: 600;
    letter-spacing: 0.15em; text-transform: uppercase; color: var(--gold); margin-bottom: 8px; }}
  .doc-link {{ display: block; font-family: var(--sans); font-size: 13px; color: var(--ink);
    text-decoration: none; font-weight: 600; margin-bottom: 4px; transition: color 0.12s; }}
  .doc-link:hover {{ color: var(--accent); }}
  .doc-desc {{ font-family: var(--sans); font-size: 11px; color: var(--muted); line-height: 1.55; }}

  /* Footer */
  footer {{ background: var(--ink); color: #555; padding: 28px 40px;
    font-family: var(--mono); font-size: 10px; letter-spacing: 0.05em;
    border-top: 3px solid var(--accent); margin-top: 80px; }}
  .footer-inner {{ max-width: var(--wide); margin: 0 auto; display: flex;
    justify-content: space-between; align-items: center; gap: 20px; flex-wrap: wrap; }}
  footer a {{ color: #aaa; text-decoration: none; }}
  footer a:hover {{ color: var(--paper); }}

  /* Responsive */
  @media (max-width: 960px) {{
    .hero {{ grid-template-columns: 1fr; }}
    .status-card {{ display: none; }}
    .intro-grid {{ grid-template-columns: 1fr; gap: 40px; }}
    .feeds-cats {{ grid-template-columns: repeat(2, 1fr); }}
    .docs-grid {{ grid-template-columns: repeat(2, 1fr); }}
  }}
  @media (max-width: 600px) {{
    .hero, .section {{ padding-left: 20px; padding-right: 20px; }}
    .masthead-inner {{ padding: 12px 20px; }}
    .masthead-subtitle {{ display: none; }}
    .nav-inner {{ padding: 0 20px; }}
    .feeds-cats {{ grid-template-columns: 1fr; }}
    .docs-grid {{ grid-template-columns: 1fr; }}
    .footer-inner {{ flex-direction: column; text-align: center; gap: 10px; }}
  }}
  @keyframes fadeUp {{
    from {{ opacity: 0; transform: translateY(14px); }}
    to   {{ opacity: 1; transform: translateY(0); }}
  }}
  .hero-text   {{ animation: fadeUp 0.6s ease both; }}
  .status-card {{ animation: fadeUp 0.6s 0.15s ease both; }}
</style>
</head>
<body>

<header class="masthead">
  <div class="masthead-inner">
    <div class="masthead-left">
      <div class="masthead-wordmark"><span>SPARK</span> &nbsp;/ DatabricksHub</div>
      <div class="masthead-divider"></div>
      <div class="masthead-subtitle">Knowledge Sharing &amp; Collaboration</div>
    </div>
    <div class="masthead-meta">
      Updated: {updated} &nbsp;|&nbsp; Daily 06:00 &nbsp;|&nbsp;
      Groq Llama 3.3 &nbsp;|&nbsp; Databricks SQL Warehouse
    </div>
  </div>
</header>

<nav class="nav">
  <div class="nav-inner">
    <a href="#about">About</a>
    <a href="#agents">How It Works</a>
    <a href="#projects">Projects</a>
    <a href="#feeds">Knowledge Feeds</a>
    <a href="#docs">Documentation</a>
  </div>
</nav>

<main>

<div class="hero" id="about">
  <div class="hero-text">
    <div class="hero-eyebrow">Self-governing Platform for Automated Research and Knowledge</div>
    <h1 class="hero-title">Meet SPARK.</h1>
    <div class="hero-tagline">Autonomous Databricks AI Engineer &mdash; DatabricksHub</div>
    <p class="hero-lead">
      Databricks ships features faster than any one person can track. SPARK was built for
      the curious practitioner who wants to <em>keep learning</em> without losing hours to
      release notes &mdash; and who believes that knowledge is most valuable when it is
      <em>shared, documented, and reproducible</em>.
    </p>
    <p class="hero-lead" style="margin-top:14px;">
      Every morning at 6 AM, SPARK reads across 28 sources, selects a Databricks feature
      worth exploring, writes a working notebook, runs it on a live SQL Warehouse, validates
      the output, publishes it to GitHub, and updates this portal. The team gains a growing,
      searchable library of hands-on demonstrations &mdash; built automatically, ready to fork and extend.
    </p>
  </div>
  <div class="status-card">
    <div class="status-card-title">System Status</div>
    <div class="stat"><span>Agent</span><span class="stat-val">SPARK v1.0</span></div>
    <div class="stat"><span>Supervisor</span><span class="stat-val">Active</span></div>
    <div class="stat"><span>Projects run</span><span class="stat-val">{run_count}</span></div>
    <div class="stat"><span>Schedule</span><span class="stat-val">Daily 06:00</span></div>
    <div class="stat"><span>LLM</span><span class="stat-val">Llama 3.3 70B</span></div>
    <div class="stat"><span>Warehouse</span><span class="stat-val">SQL (Community)</span></div>
    <div class="stat"><span>Publisher</span><span class="stat-val">GitHub</span></div>
    <div class="stat"><span>Sources</span><span class="stat-val">28 feeds</span></div>
  </div>
</div>

<section class="section" id="agents">
  <div class="intro-grid">
    <div class="intro-body">
      <h2 class="section-title">Built for Anyone Who Wants to Keep Learning</h2>
      <p>
        Keeping pace with a platform that evolves as rapidly as Databricks demands
        constant attention. New features land weekly &mdash; Delta Lake, MLflow, Unity Catalog,
        Liquid Clustering, Serverless Compute, Mosaic AI. Each one worth exploring.
        Rarely enough hours to explore any of them properly.
      </p>
      <div class="pull-quote">
        "What if the research, the notebook, and the write-up happened automatically &mdash; so the
        team could spend time learning from results rather than producing them?"
      </div>
      <p>
        SPARK turns that question into a daily practice. A <strong>Supervisor Agent</strong>
        orchestrates six specialised agents in sequence. Each reads from the previous,
        adds its contribution, and passes a shared context forward. If any step fails,
        the pipeline halts and reports precisely why.
      </p>
      <p>
        Every successful run produces a validated, committed, documented Databricks notebook &mdash;
        ready for the whole team to read, run, and build on. Knowledge is created once
        and shared openly.
      </p>
    </div>
    <div>
      <h2 class="section-title">The Six-Agent Pipeline</h2>
      <div class="agent-flow">
        <div class="agent-node">
          <div class="agent-connector"><div class="agent-num">01</div><div class="agent-line"></div></div>
          <div class="agent-body">
            <div class="agent-name">Knowledge Agent</div>
            <div class="agent-desc">Scrapes 28 sources &mdash; official docs, Stack Overflow, Reddit, Medium, Hacker News, LinkedIn, and Google News &mdash; collecting relevant articles each morning.</div>
            <div class="agent-output">Output: articles[]</div>
          </div>
        </div>
        <div class="agent-node">
          <div class="agent-connector"><div class="agent-num">02</div><div class="agent-line"></div></div>
          <div class="agent-body">
            <div class="agent-name">Feature Analyser</div>
            <div class="agent-desc">Reviews collected articles and selects the most compelling Databricks feature to demonstrate, avoiding topics already covered in previous runs.</div>
            <div class="agent-output">Output: feature, description, project_idea, tags</div>
          </div>
        </div>
        <div class="agent-node">
          <div class="agent-connector"><div class="agent-num">03</div><div class="agent-line"></div></div>
          <div class="agent-body">
            <div class="agent-name">Project Generator</div>
            <div class="agent-desc">Generates a Databricks SQL notebook, companion queries, and a README &mdash; saved under a dated project schema in the format YYYYMMDD_feature.</div>
            <div class="agent-output">Output: notebook.py, queries.sql, README.md</div>
          </div>
        </div>
        <div class="agent-node">
          <div class="agent-connector"><div class="agent-num">04</div><div class="agent-line"></div></div>
          <div class="agent-body">
            <div class="agent-name">Databricks Executor</div>
            <div class="agent-desc">Uploads the notebook, classifies each cell using the LLM, converts Python cells to SQL for the Warehouse, executes all cells, and retries any that fail with an auto-generated fix.</div>
            <div class="agent-output">Output: run_state, run_output, notebook_url</div>
          </div>
        </div>
        <div class="agent-node">
          <div class="agent-connector"><div class="agent-num">05</div><div class="agent-line"></div></div>
          <div class="agent-body">
            <div class="agent-name">Validation Agent</div>
            <div class="agent-desc">Analyses execution output and scores the demo from 1 to 10. Errors resolved during retry are not penalised. A minimum score of 7 is required to proceed to publication.</div>
            <div class="agent-output">Output: quality_score, validated, issues[]</div>
          </div>
        </div>
        <div class="agent-node">
          <div class="agent-connector"><div class="agent-num">06</div><div class="agent-line"></div></div>
          <div class="agent-body">
            <div class="agent-name">Publisher Agent</div>
            <div class="agent-desc">Commits the project to the DatabricksHub GitHub repository and updates this knowledge portal with a new entry in the daily projects table.</div>
            <div class="agent-output">Output: github_url, commit_sha</div>
          </div>
        </div>
      </div>
    </div>
  </div>
</section>

<section class="section" id="projects">
  <h2 class="section-title">Daily Projects</h2>
  <p style="font-family:var(--sans);font-size:13px;color:var(--muted);max-width:720px;line-height:1.7;margin:0 auto 4px;text-align:left;">
    Each entry is a validated, committed Databricks notebook exploring a specific platform feature.
    Schemas follow the convention
    <code style="font-family:var(--mono);font-size:11px;background:#e8e4da;padding:1px 6px;">daily_projects.YYYYMMDD_feature</code>.
    All notebooks are available on GitHub for the team to review and reuse.
  </p>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Date</th><th>Feature</th><th>Project</th>
          <th>Schema</th><th>Notebook</th><th>GitHub</th><th>Score</th>
        </tr>
      </thead>
      <tbody>
{proj_rows}
      </tbody>
    </table>
  </div>
</section>

<section class="section" id="feeds">
  <h2 class="section-title">Where SPARK Reads</h2>
  <p style="font-family:var(--sans);font-size:13px;color:var(--muted);max-width:680px;line-height:1.7;margin:0 auto;text-align:left;">
    Every morning the Knowledge Agent reads from the following sources before selecting a feature to build.
    Sources span official documentation, developer communities, publications, and social channels.
  </p>
  <div class="feeds-cats">
{feed_html}
  </div>
</section>

<section class="section" id="docs">
  <h2 class="section-title">Databricks Documentation</h2>
  <p style="font-family:var(--sans);font-size:13px;color:var(--muted);max-width:680px;line-height:1.7;margin:0 auto;text-align:left;">
    Canonical reference material for the Databricks platform &mdash; documentation, release notes,
    architecture guides, API reference, blogs, and research.
  </p>
  <div class="docs-grid">
    <div class="doc-card"><div class="doc-cat">Getting Started</div>
      <a class="doc-link" href="https://docs.databricks.com/" target="_blank">Databricks Documentation</a>
      <div class="doc-desc">Entry point for all platform documentation across AWS, Azure, and GCP.</div></div>
    <div class="doc-card"><div class="doc-cat">Release Notes</div>
      <a class="doc-link" href="https://docs.databricks.com/release-notes/product/index.html" target="_blank">Platform Release Notes</a>
      <div class="doc-desc">New features, deprecations, and fixes by platform release.</div></div>
    <div class="doc-card"><div class="doc-cat">Release Notes</div>
      <a class="doc-link" href="https://docs.databricks.com/sql/release-notes/index.html" target="_blank">Databricks SQL Release Notes</a>
      <div class="doc-desc">SQL warehouse and serverless SQL updates by version.</div></div>
    <div class="doc-card"><div class="doc-cat">Release Notes</div>
      <a class="doc-link" href="https://docs.databricks.com/release-notes/runtime/index.html" target="_blank">Databricks Runtime Release Notes</a>
      <div class="doc-desc">DBR version history including Spark, Python, and library versions.</div></div>
    <div class="doc-card"><div class="doc-cat">Architecture</div>
      <a class="doc-link" href="https://docs.databricks.com/lakehouse/index.html" target="_blank">Lakehouse Architecture</a>
      <div class="doc-desc">Delta Lake, Unity Catalog, and the Medallion architecture explained.</div></div>
    <div class="doc-card"><div class="doc-cat">Architecture</div>
      <a class="doc-link" href="https://docs.databricks.com/data-governance/unity-catalog/index.html" target="_blank">Unity Catalog</a>
      <div class="doc-desc">Unified governance &mdash; schemas, tables, lineage, and access controls.</div></div>
    <div class="doc-card"><div class="doc-cat">Delta Lake</div>
      <a class="doc-link" href="https://docs.databricks.com/delta/index.html" target="_blank">Delta Lake Guide</a>
      <div class="doc-desc">ACID transactions, time travel, schema evolution, MERGE, OPTIMIZE.</div></div>
    <div class="doc-card"><div class="doc-cat">Delta Lake</div>
      <a class="doc-link" href="https://delta.io/" target="_blank">Delta Lake Open Source</a>
      <div class="doc-desc">Protocol specification and community resources for open-source Delta.</div></div>
    <div class="doc-card"><div class="doc-cat">Machine Learning</div>
      <a class="doc-link" href="https://mlflow.org/docs/latest/index.html" target="_blank">MLflow Documentation</a>
      <div class="doc-desc">Experiment tracking, model registry, model serving, and the MLflow API.</div></div>
    <div class="doc-card"><div class="doc-cat">Machine Learning</div>
      <a class="doc-link" href="https://docs.databricks.com/machine-learning/index.html" target="_blank">Databricks ML Guide</a>
      <div class="doc-desc">AutoML, feature store, model serving, and Mosaic AI on Databricks.</div></div>
    <div class="doc-card"><div class="doc-cat">Streaming</div>
      <a class="doc-link" href="https://docs.databricks.com/structured-streaming/index.html" target="_blank">Structured Streaming</a>
      <div class="doc-desc">Real-time data processing with Spark Structured Streaming on Databricks.</div></div>
    <div class="doc-card"><div class="doc-cat">Developer</div>
      <a class="doc-link" href="https://docs.databricks.com/dev-tools/api/index.html" target="_blank">REST API Reference</a>
      <div class="doc-desc">Jobs, clusters, workspace, SQL statements, and all Databricks REST APIs.</div></div>
    <div class="doc-card"><div class="doc-cat">Developer</div>
      <a class="doc-link" href="https://docs.databricks.com/dev-tools/databricks-connect.html" target="_blank">Databricks Connect</a>
      <div class="doc-desc">Run Spark code locally against a remote Databricks cluster from any IDE.</div></div>
    <div class="doc-card"><div class="doc-cat">Developer</div>
      <a class="doc-link" href="https://docs.databricks.com/dev-tools/bundles/index.html" target="_blank">Databricks Asset Bundles</a>
      <div class="doc-desc">CI/CD for Databricks &mdash; deploy jobs, notebooks, and pipelines as code.</div></div>
    <div class="doc-card"><div class="doc-cat">Blog</div>
      <a class="doc-link" href="https://www.databricks.com/blog/category/engineering" target="_blank">Engineering Blog</a>
      <div class="doc-desc">Deep technical articles from the Databricks engineering team.</div></div>
    <div class="doc-card"><div class="doc-cat">Blog</div>
      <a class="doc-link" href="https://www.databricks.com/blog/category/company" target="_blank">Company Blog</a>
      <div class="doc-desc">Product announcements, partner news, and strategic direction.</div></div>
    <div class="doc-card"><div class="doc-cat">Research</div>
      <a class="doc-link" href="https://www.databricks.com/research" target="_blank">Databricks Research</a>
      <div class="doc-desc">Published papers from the Databricks and Mosaic AI research teams.</div></div>
    <div class="doc-card"><div class="doc-cat">Community</div>
      <a class="doc-link" href="https://community.databricks.com/" target="_blank">Databricks Community</a>
      <div class="doc-desc">Q&amp;A, technical blogs, and peer discussion across the user community.</div></div>
  </div>
</section>

</main>

<footer>
  <div class="footer-inner">
    <div>SPARK &mdash; DatabricksHub Knowledge Portal &mdash; Autonomous Databricks AI Engineer</div>
    <div>Built for anyone who wants to keep learning and share knowledge simply</div>
    <div>Updated automatically every day at 06:00 &nbsp;|&nbsp;
      <a href="https://github.com/{cfg.github_repo}" target="_blank">GitHub</a></div>
  </div>
</footer>

</body>
</html>"""

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _project_rows(self, history: list[dict]) -> str:
        if not history:
            return (
                '        <tr><td colspan="7" style="text-align:center;'
                'font-family:var(--mono);font-size:12px;color:#aaa;padding:40px;">'
                'No projects run yet. First run coming soon.</td></tr>'
            )
        rows = []
        for h in history:
            date = h.get("date", "")
            if len(date) == 8:
                date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
            nb_url  = h.get("notebook_url", "")
            gh_url  = h.get("github_url", "")
            nb_link = (f'<a class="table-link" href="{nb_url}" target="_blank">Notebook</a>'
                       if nb_url else '<span class="badge">pending</span>')
            gh_link = (f'<a class="table-link" href="{gh_url}" target="_blank">README</a>'
                       if gh_url else '<span class="badge">pending</span>')
            score   = h.get("quality_score", 0)
            score_d = f'<span class="badge">{score}/10</span>' if score else ""
            rows.append(
                f'        <tr>'
                f'<td class="date-col">{date}</td>'
                f'<td class="feature-col">{h.get("feature","")}</td>'
                f'<td class="project-col">{h.get("project_idea","")[:70]}</td>'
                f'<td class="schema-col">{h.get("schema","")}</td>'
                f'<td>{nb_link}</td><td>{gh_link}</td><td>{score_d}</td>'
                f'</tr>'
            )
        return "\n".join(rows)

    def _feed_html(self) -> str:
        cats = [
            ("Official Databricks", [
                (url, url.replace("https://","").replace("www.","").split("/")[0]
                 + ("/" + url.split(".com/",1)[-1].rstrip("/") if ".com/" in url else ""))
                for url in cfg.knowledge_sources_official
            ] + [(e["url"], e["label"]) for e in cfg.knowledge_sources_rss
                 if "databricks.com" in e.get("url","") or "databricks.com" in e.get("label","").lower()]),

            ("Developer Community", [
                (url, "Stack Overflow: " + url.split("/tagged/")[-1].replace("+", " + "))
                for url in cfg.knowledge_sources_community
            ] + [(e["url"], e["label"]) for e in cfg.knowledge_sources_rss
                 if any(x in e.get("url","") for x in ["reddit", "hnrss", "dev.to"])]),

            ("Articles &amp; Publications", [
                (url, url.replace("https://","").split("/feed/")[0].replace("www.",""))
                for url in cfg.knowledge_sources_articles
            ] + [(e["url"], e["label"]) for e in cfg.knowledge_sources_rss
                 if any(x in e.get("url","") for x in ["medium", "towardsdatascience", "dataengineeringweekly"])]),

            ("LinkedIn &amp; Google News", [
                (e["url"], e["label"]) for e in cfg.knowledge_sources_rss
                if "linkedin" in e.get("label","").lower() or "rss.app" in e.get("url","")
            ] + [(f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}", q)
                 for q in cfg.knowledge_sources_google_news]),
        ]

        parts = []
        for title, links in cats:
            items = "\n".join(
                f'      <a class="feed-link" href="{url}" target="_blank">{label}</a>'
                for url, label in links[:12]
            )
            parts.append(
                f'    <div>\n'
                f'      <div class="feeds-cat-title">{title}</div>\n'
                f'{items}\n'
                f'    </div>'
            )
        return "\n".join(parts)