"""
config.py — Central configuration for the Databricks AI Engineer system.
All settings are loaded from environment variables with sensible defaults.
"""

import os
from dataclasses import dataclass, field


@dataclass
class Config:
    # ── LLM Provider ───────────────────────────────────────────────────────────
    llm_provider: str = field(
        default_factory=lambda: os.environ.get("LLM_PROVIDER", "groq")
    )

    # ── Groq (FREE — recommended for students) ─────────────────────────────────
    groq_api_key: str = field(
        default_factory=lambda: os.environ.get("GROQ_API_KEY", "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")
    )
    groq_model: str = "llama-3.3-70b-versatile"

    # ── Google Gemini (FREE tier) ──────────────────────────────────────────────
    gemini_api_key: str = field(
        default_factory=lambda: os.environ.get("GEMINI_API_KEY", "")
    )
    gemini_model: str = "gemini-1.5-flash"

    # ── Ollama (LOCAL — 100% free) ─────────────────────────────────────────────
    ollama_base_url: str = field(
        default_factory=lambda: os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
    )
    ollama_model: str = "llama3"

    # ── Anthropic (Paid — best quality) ───────────────────────────────────────
    anthropic_api_key: str = field(
        default_factory=lambda: os.environ.get("ANTHROPIC_API_KEY", "")
    )
    anthropic_model: str = "claude-sonnet-4-6"

    # ── Databricks ─────────────────────────────────────────────────────────────
    databricks_host: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_HOST", "")
    )
    databricks_token: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_TOKEN", "")
    )
    databricks_cluster_id: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_CLUSTER_ID", "")
    )
    databricks_workspace_path: str = "/Shared/AI_Engineer_Demos"
    databricks_http_path: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_HTTP_PATH", "")
    )
    databricks_parent_schema: str = "daily_projects"

    # ── GitHub ─────────────────────────────────────────────────────────────────
    github_token: str = field(
        default_factory=lambda: os.environ.get("GITHUB_TOKEN", "")
    )
    github_repo: str = field(
        default_factory=lambda: os.environ.get(
            "GITHUB_REPO", "your-username/databricks-daily-ai-engineer"
        )
    )
    github_branch: str = "main"

    # ── Knowledge Sources ──────────────────────────────────────────────────────
    # 1. Official Databricks
    knowledge_sources_official: list = field(default_factory=lambda: [
        "https://www.databricks.com/blog",
        "https://docs.databricks.com/release-notes/",
        "https://community.databricks.com/",
        "https://www.databricks.com/solutions",
    ])

    # 2. Stack Overflow (Reddit removed — blocks scrapers, use RSS below instead)
    knowledge_sources_community: list = field(default_factory=lambda: [
        "https://stackoverflow.com/questions/tagged/databricks",
        "https://stackoverflow.com/questions/tagged/apache-spark+databricks",
    ])

    # 3. Article sites (Medium removed — blocks scrapers, use RSS below instead)
    knowledge_sources_articles: list = field(default_factory=lambda: [
        "https://towardsdatascience.com/tagged/databricks",
    ])

    # 4. Google News RSS — searches LinkedIn, blogs, news, forums
    knowledge_sources_google_news: list = field(default_factory=lambda: [
        "Databricks site:linkedin.com",
        "Databricks new feature announcement",
        "Databricks Delta Lake tutorial",
        "Apache Spark Databricks best practices",
        "Databricks Unity Catalog",
        "Databricks MLflow machine learning",
    ])

    # 5. RSS feeds — direct feeds, no login needed
    #    Reddit and Medium block HTML scraping but their RSS feeds work fine
    knowledge_sources_rss: list = field(default_factory=lambda: [
        # Official Databricks
        {"url": "https://www.databricks.com/feed",
         "label": "Databricks Blog (Official)"},
        {"url": "https://community.databricks.com/t5/technical-blog/rss/board",
         "label": "Databricks Community Technical Blog"},
        # Reddit via RSS (works even though HTML scraping is blocked)
        {"url": "https://www.reddit.com/r/databricks/.rss",
         "label": "Reddit r/databricks"},
        {"url": "https://www.reddit.com/r/apachespark/.rss",
         "label": "Reddit r/apachespark"},
        {"url": "https://www.reddit.com/r/dataengineering/.rss",
         "label": "Reddit r/dataengineering"},
        # Medium via RSS (works even though HTML scraping is blocked)
        {"url": "https://medium.com/feed/tag/databricks",
         "label": "Medium: #databricks"},
        {"url": "https://medium.com/feed/tag/apache-spark",
         "label": "Medium: #apache-spark"},
        {"url": "https://medium.com/feed/databricks",
         "label": "Medium: Databricks publication"},
        # Data engineering community
        {"url": "https://www.dataengineeringweekly.com/feed",
         "label": "Data Engineering Weekly"},
        {"url": "https://towardsdatascience.com/feed",
         "label": "Towards Data Science"},
        # Hacker News keyword feeds
        {"url": "https://hnrss.org/newest?q=databricks&points=10",
         "label": "Hacker News: Databricks"},
        {"url": "https://hnrss.org/newest?q=delta+lake&points=10",
         "label": "Hacker News: Delta Lake"},
        # Dev.to
        {"url": "https://dev.to/feed/tag/databricks",
         "label": "Dev.to #databricks"},
        {"url": "https://dev.to/feed/tag/apachespark",
         "label": "Dev.to #apachespark"},
        # LinkedIn Databricks company page (via rss.app)
        {"url": "https://rss.app/feed/AC7IAplgI5NhIp45",
         "label": "LinkedIn: Databricks Company Page"},
    ])

    # ── Paths ──────────────────────────────────────────────────────────────────
    history_file: str = "history.json"
    projects_dir: str = "projects"
    log_level: str   = "INFO"

    # ── Execution ──────────────────────────────────────────────────────────────
    job_timeout_seconds: int     = 600
    job_poll_interval_seconds: int = 15


# Singleton instance used across all agents
cfg = Config()