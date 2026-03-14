# 🤖 Databricks Daily AI Engineer

An **autonomous multi-agent system** that continuously discovers new Databricks features,
generates working demo projects, deploys them to a live Databricks workspace, validates
execution, and publishes the results to GitHub — completely hands-free.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       SUPERVISOR AGENT                      │
│              (orchestrates the pipeline below)              │
└──────────────────────────┬──────────────────────────────────┘
                           │ context dict flows downward
           ┌───────────────▼───────────────┐
           │        Knowledge Agent        │  scrapes Databricks blog + community
           └───────────────┬───────────────┘
           ┌───────────────▼───────────────┐
           │     Feature Analyzer Agent    │  Claude picks next feature to demo
           └───────────────┬───────────────┘
           ┌───────────────▼───────────────┐
           │    Project Generator Agent    │  Claude writes notebook.py + SQL + README
           └───────────────┬───────────────┘
           ┌───────────────▼───────────────┐
           │   Databricks Executor Agent   │  uploads + runs notebook via Jobs API
           └───────────────┬───────────────┘
           ┌───────────────▼───────────────┐
           │      Validation Agent         │  Claude analyses run output quality
           └───────────────┬───────────────┘
           ┌───────────────▼───────────────┐
           │       Publisher Agent         │  commits validated project to GitHub
           └───────────────────────────────┘
```

## Project Structure

```
databricks_ai_engineer/
├── main.py                         # Entry point (CLI)
├── supervisor.py                   # Supervisor Agent — pipeline orchestrator
├── config.py                       # All configuration (env-var driven)
├── requirements.txt
├── history.json                    # Auto-generated: list of demonstrated features
├── reports/                        # Auto-generated: per-run JSON reports
│   └── 20260308-143022.json
├── projects/                       # Auto-generated: demo project files
│   └── 20260308-lakebase/
│       ├── notebook.py
│       ├── queries.sql
│       └── README.md
├── agents/
│   ├── base_agent.py               # Abstract base class for all agents
│   ├── knowledge_agent.py          # Web scraper
│   ├── feature_analyzer.py         # LLM-powered feature selection
│   ├── project_generator.py        # LLM-powered code generation
│   ├── databricks_executor.py      # Databricks REST API integration
│   ├── validation_agent.py         # LLM-powered quality gate
│   └── publisher_agent.py          # GitHub REST API integration
└── utils/
    ├── logger.py                   # Coloured structured logger
    └── claude_client.py            # Anthropic SDK wrapper with retry logic
```

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set environment variables

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
export DATABRICKS_HOST="https://adb-xxxxxxxxxx.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_CLUSTER_ID="0123-456789-abc"    # optional but faster
export GITHUB_TOKEN="ghp_..."
export GITHUB_REPO="your-username/databricks-daily-ai-engineer"
```

Or copy `.env.example` to `.env` and fill in the values, then:

```bash
pip install python-dotenv
# add `from dotenv import load_dotenv; load_dotenv()` to main.py
```

### 3. Run

```bash
# Full pipeline run
python main.py

# Dry-run (no Databricks / GitHub — great for testing locally)
python main.py --dry-run

# Daemon mode: run every 24 hours
python main.py --schedule 24

# Full run with retry on failures
python main.py --max-retries 2
```

## Agent Reference

| Agent | Input (from context) | Output (to context) |
|-------|---------------------|---------------------|
| KnowledgeAgent | `knowledge_sources` | `articles` |
| FeatureAnalyzerAgent | `articles` | `feature`, `description`, `project_idea`, `tags` |
| ProjectGeneratorAgent | `feature`, `description`, `project_idea` | `project_dir`, `notebook_path`, `sql_path`, `readme_path`, `slug`, `date_str` |
| DatabricksExecutorAgent | `notebook_path`, `feature`, `slug`, `date_str` | `run_id`, `run_url`, `run_state`, `run_output` |
| ValidationAgent | `run_state`, `run_output`, `feature` | `validated`, `quality_score`, `summary`, `issues` |
| PublisherAgent | `project_dir`, `feature`, `slug`, `date_str`, `quality_score`, `run_url` | `github_url`, `commit_sha`, `commit_url` |

## Extending the System

### Add a new agent

1. Create `agents/my_new_agent.py` inheriting from `BaseAgent`
2. Implement `_run(self, context) -> AgentResult`
3. Add it to the `self._agents` list in `supervisor.py`

### Add a new knowledge source

Edit `config.py`:

```python
knowledge_sources: list = field(default_factory=lambda: [
    "https://www.databricks.com/blog",
    "https://your-new-source.com/",
])
```

### Self-healing (future)

The supervisor's `_run_with_retries` method is already in place.
Add a `HealingAgent` between Validation and Publisher that uses Claude
to diagnose and patch the notebook, then re-submit to the Executor.

## Output

Each successful run produces:

```
projects/20260308-lakebase/
    notebook.py     ← full Databricks Python notebook
    queries.sql     ← companion SQL queries
    README.md       ← project documentation

reports/20260308-143022.json   ← pipeline execution report

history.json       ← updated with the new feature name

GitHub:
    projects/20260308-lakebase/   ← published to your repo
    PROJECTS.md                   ← updated index
```
