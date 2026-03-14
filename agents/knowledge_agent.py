"""
agents/knowledge_agent.py — Knowledge Agent

Scrapes multiple source categories for the latest Databricks content:
  - Official  : databricks.com blog, docs release notes, community
  - Community : Stack Overflow, Reddit r/databricks, r/apachespark
  - Articles  : Medium, Towards Data Science

Each source type has a tailored scraper so we get real content,
not just nav bars.

Output context keys:
    articles — list of dicts: {title, url, snippet, source_type, source}
"""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from agents.base_agent import BaseAgent, AgentResult
from config import cfg


# How many articles to collect per source category
MAX_PER_SOURCE   = 5
REQUEST_TIMEOUT  = 12
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


class KnowledgeAgent(BaseAgent):

    def __init__(self):
        super().__init__("Knowledge Agent")

    def _run(self, context: dict[str, Any]) -> AgentResult:
        articles: list[dict] = []

        # ── Official Databricks sources ────────────────────────────────────────
        self.logger.info("Scraping official Databricks sources …")
        for url in cfg.knowledge_sources_official:
            try:
                items = self._scrape_official(url)
                self.logger.info(f"  {url} → {len(items)} articles")
                articles.extend(items)
            except Exception as e:
                self.logger.warning(f"  {url} failed: {e}")

        # ── Stack Overflow ─────────────────────────────────────────────────────
        self.logger.info("Scraping Stack Overflow …")
        for url in cfg.knowledge_sources_community:
            if "stackoverflow" in url:
                try:
                    items = self._scrape_stackoverflow(url)
                    self.logger.info(f"  {url} → {len(items)} questions")
                    articles.extend(items)
                except Exception as e:
                    self.logger.warning(f"  {url} failed: {e}")

        # ── Reddit ─────────────────────────────────────────────────────────────
        self.logger.info("Scraping Reddit …")
        for url in cfg.knowledge_sources_community:
            if "reddit" in url:
                try:
                    items = self._scrape_reddit(url)
                    self.logger.info(f"  {url} → {len(items)} posts")
                    articles.extend(items)
                except Exception as e:
                    self.logger.warning(f"  {url} failed: {e}")

        # ── Medium / Towards Data Science ─────────────────────────────────────
        self.logger.info("Scraping Medium & TDS …")
        for url in cfg.knowledge_sources_articles:
            try:
                items = self._scrape_medium(url)
                self.logger.info(f"  {url} → {len(items)} articles")
                articles.extend(items)
            except Exception as e:
                self.logger.warning(f"  {url} failed: {e}")

        # ── Google News (LinkedIn + broad Databricks coverage) ─────────────────
        self.logger.info("Scraping Google News …")
        google_queries = cfg.knowledge_sources_google_news
        for query in google_queries:
            try:
                items = self._scrape_google_news(query)
                self.logger.info(f"  Google News '{query}' → {len(items)} results")
                articles.extend(items)
            except Exception as e:
                self.logger.warning(f"  Google News '{query}' failed: {e}")

        # ── RSS Feeds (LinkedIn via rss.app, custom feeds) ─────────────────────
        rss_feeds = cfg.knowledge_sources_rss
        if rss_feeds:
            self.logger.info(f"Reading {len(rss_feeds)} RSS feeds …")
            for feed in rss_feeds:
                url   = feed if isinstance(feed, str) else feed.get("url", "")
                label = feed.get("label", url) if isinstance(feed, dict) else url
                try:
                    items = self._scrape_rss(url, label)
                    self.logger.info(f"  RSS '{label}' → {len(items)} items")
                    articles.extend(items)
                except Exception as e:
                    self.logger.warning(f"  RSS '{label}' failed: {e}")
        else:
            self.logger.info(
                "No RSS feeds configured. To add LinkedIn: "
                "1. Go to rss.app, create a free feed for a LinkedIn hashtag/company. "
                "2. Add the RSS URL to knowledge_sources_rss in config.py"
            )

        if not articles:
            return AgentResult(
                agent=self.name, success=False,
                error="No articles collected from any source.",
            )

        self.logger.info(
            f"Total collected: {len(articles)} articles from "
            f"{len(set(a['source_type'] for a in articles))} source types"
        )
        return AgentResult(
            agent=self.name, success=True,
            data={"articles": articles},
        )

    # ── Scrapers ───────────────────────────────────────────────────────────────

    def _scrape_official(self, url: str) -> list[dict]:
        """Scrapes databricks.com blog, docs, community pages."""
        soup  = self._fetch(url)
        items = []

        # Try multiple link-finding strategies
        selectors = [
            ("a", {"class": re.compile(r"blog|post|article|card|title", re.I)}),
            ("h2", {}), ("h3", {}),
        ]

        seen = set()
        for tag, attrs in selectors:
            for el in soup.find_all(tag, attrs, limit=20):
                anchor = el if el.name == "a" else el.find("a")
                if not anchor:
                    continue
                href  = anchor.get("href", "")
                title = anchor.get_text(strip=True)
                if not href or not title or len(title) < 10:
                    continue
                full_url = urljoin(url, href)
                if full_url in seen:
                    continue
                seen.add(full_url)
                # Get snippet from nearby <p>
                snippet = ""
                parent = anchor.find_parent(["div", "article", "li"])
                if parent:
                    p = parent.find("p")
                    snippet = p.get_text(strip=True)[:200] if p else ""
                items.append({
                    "title":       title,
                    "url":         full_url,
                    "snippet":     snippet,
                    "source":      url,
                    "source_type": "official",
                })
                if len(items) >= MAX_PER_SOURCE:
                    break
            if len(items) >= MAX_PER_SOURCE:
                break

        return items

    def _scrape_stackoverflow(self, url: str) -> list[dict]:
        """Scrapes Stack Overflow questions tagged databricks/spark."""
        # Use the JSON API — no scraping needed, no login required
        tag = urlparse(url).path.split("/tagged/")[-1]
        api_url = (
            f"https://api.stackexchange.com/2.3/questions"
            f"?order=desc&sort=activity&tagged={tag}"
            f"&site=stackoverflow&pagesize={MAX_PER_SOURCE}&filter=withbody"
        )
        try:
            resp = requests.get(api_url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            # Fallback to HTML scraping if API fails
            return self._scrape_stackoverflow_html(url)

        items = []
        for q in data.get("items", [])[:MAX_PER_SOURCE]:
            body_html = q.get("body", "")
            body_text = BeautifulSoup(body_html, "html.parser").get_text(" ", strip=True)[:200]
            items.append({
                "title":       q.get("title", ""),
                "url":         q.get("link", ""),
                "snippet":     body_text,
                "source":      url,
                "source_type": "stackoverflow",
            })
        return items

    def _scrape_stackoverflow_html(self, url: str) -> list[dict]:
        soup  = self._fetch(url)
        items = []
        for el in soup.select(".s-post-summary", limit=MAX_PER_SOURCE):
            a       = el.select_one(".s-link")
            excerpt = el.select_one(".s-post-summary--content-excerpt")
            if not a:
                continue
            items.append({
                "title":       a.get_text(strip=True),
                "url":         urljoin(url, a.get("href", "")),
                "snippet":     excerpt.get_text(strip=True)[:200] if excerpt else "",
                "source":      url,
                "source_type": "stackoverflow",
            })
        return items

    def _scrape_reddit(self, url: str) -> list[dict]:
        """
        Reddit blocks HTML scraping with 403.
        Use the RSS feed instead — Reddit provides .rss for every subreddit.
        This method is kept for backward compat but redirects to RSS.
        """
        rss_url = url.rstrip("/") + "/.rss"
        return self._scrape_rss(rss_url, f"Reddit: {url.split('/')[-2]}")

    def _scrape_medium(self, url: str) -> list[dict]:
        """
        Medium blocks HTML scraping with 403.
        Convert the tag/publication URL to its RSS feed URL instead.
        e.g. medium.com/tag/databricks -> medium.com/feed/tag/databricks
        """
        # Convert to RSS URL
        if "medium.com" in url:
            parsed = url.replace("medium.com/", "medium.com/feed/")
            rss_url = parsed if "/feed/" in parsed else url.replace("medium.com/", "medium.com/feed/")
        else:
            # towardsdatascience — use their main feed
            rss_url = "https://towardsdatascience.com/feed"
        label = url.split("medium.com/")[-1] if "medium.com" in url else "TDS"
        items = self._scrape_rss(rss_url, f"Medium: {label}")
        # Filter to Databricks-relevant only
        filtered = [
            i for i in items
            if re.search(r"databricks|spark|delta|mlflow|unity.catalog|lakehouse", i["title"], re.I)
        ]
        return filtered if filtered else items[:MAX_PER_SOURCE]

    # ── Google News search ────────────────────────────────────────────────────

    def _scrape_google_news(self, query: str) -> list[dict]:
        """
        Searches Google News RSS for a query term.
        Returns articles from LinkedIn, Medium, forums etc. indexed by Google.
        No login or API key required.
        """
        import urllib.parse
        encoded = urllib.parse.quote(query)
        rss_url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

        try:
            resp = requests.get(rss_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning(f"Google News RSS failed for '{query}': {e}")
            return []

        try:
            soup = BeautifulSoup(resp.text, "lxml-xml")
        except Exception:
            soup = BeautifulSoup(resp.text, "html.parser")
        items = []
        for item in soup.find_all("item")[:MAX_PER_SOURCE]:
            title  = item.find("title")
            link   = item.find("link")
            desc   = item.find("description")
            source = item.find("source")

            title_text  = title.get_text(strip=True)  if title  else ""
            link_text   = link.get_text(strip=True)   if link   else ""
            desc_text   = BeautifulSoup(desc.get_text(strip=True), "html.parser").get_text(strip=True)[:200] if desc else ""
            source_text = source.get_text(strip=True) if source else "Google News"

            if not title_text:
                continue

            items.append({
                "title":       title_text,
                "url":         link_text,
                "snippet":     desc_text,
                "source":      f"Google News: {query}",
                "source_type": "google_news",
                "via":         source_text,
            })
        return items

    # ── RSS feed reader ────────────────────────────────────────────────────────

    def _scrape_rss(self, rss_url: str, source_label: str = "") -> list[dict]:
        """
        Generic RSS/Atom feed reader.
        Works with any public RSS feed including:
          - rss.app LinkedIn feeds
          - fetchrss.com feeds
          - Any Databricks/tech RSS feed
        """
        try:
            resp = requests.get(rss_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning(f"RSS feed failed ({rss_url}): {e}")
            return []

        # Use lxml-xml if available, fall back to html.parser
        try:
            soup = BeautifulSoup(resp.text, "lxml-xml")
        except Exception:
            soup = BeautifulSoup(resp.text, "html.parser")
        items = []
        label = source_label or rss_url

        # Handle both RSS <item> and Atom <entry> formats
        entries = soup.find_all("item") or soup.find_all("entry")

        for entry in entries[:MAX_PER_SOURCE]:
            title   = entry.find("title")
            link    = entry.find("link")
            summary = entry.find("summary") or entry.find("description") or entry.find("content")

            title_text = title.get_text(strip=True) if title else ""
            # Atom <link> uses href attribute, RSS uses text content
            if link:
                link_text = link.get("href") or link.get_text(strip=True)
            else:
                link_text = ""
            summary_text = ""
            if summary:
                raw = summary.get_text(strip=True)
                summary_text = BeautifulSoup(raw, "html.parser").get_text(strip=True)[:200]

            if not title_text:
                continue

            items.append({
                "title":       title_text,
                "url":         link_text,
                "snippet":     summary_text,
                "source":      label,
                "source_type": "rss",
            })
        return items

    # ── HTTP helper ────────────────────────────────────────────────────────────

    def _fetch(self, url: str) -> BeautifulSoup:
        time.sleep(0.5)   # polite delay
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")