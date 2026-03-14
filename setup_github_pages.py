"""
setup_github_pages.py — One-time script to enable GitHub Pages for the repo.

Run once:
    python setup_github_pages.py

After this, the portal will be live at:
    https://DatabricksHub.github.io/databricks-daily-ai-engineer/

GitHub Pages will serve index.html from the root of the main branch.
Every daily run will update index.html automatically.
"""

import os
import sys
import requests
from pathlib import Path

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = os.environ.get("GITHUB_REPO", "DatabricksHub/databricks-daily-ai-engineer")
BRANCH       = "main"

if not GITHUB_TOKEN:
    print("[ERROR] GITHUB_TOKEN not set in .env")
    sys.exit(1)

owner, repo = GITHUB_REPO.split("/", 1)

headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

# ── Step 1: Enable GitHub Pages ────────────────────────────────────────────────
print(f"\nEnabling GitHub Pages for {GITHUB_REPO} ...")
url  = f"https://api.github.com/repos/{owner}/{repo}/pages"

# Check if already enabled
check = requests.get(url, headers=headers)
if check.status_code == 200:
    page_url = check.json().get("html_url", "")
    print(f"[OK] GitHub Pages already enabled: {page_url}")
else:
    # Enable it
    resp = requests.post(url, headers=headers, json={
        "source": {"branch": BRANCH, "path": "/"},
        "build_type": "legacy",
    })
    if resp.status_code in (201, 200):
        page_url = resp.json().get("html_url", f"https://{owner}.github.io/{repo}/")
        print(f"[OK] GitHub Pages enabled: {page_url}")
    else:
        print(f"[ERROR] Could not enable Pages: {resp.status_code} {resp.text}")
        print("\nTo enable manually:")
        print(f"  1. Go to https://github.com/{GITHUB_REPO}/settings/pages")
        print(f"  2. Source: Deploy from a branch")
        print(f"  3. Branch: main  /  Folder: / (root)")
        print(f"  4. Click Save")
        sys.exit(1)

# ── Step 2: Upload current index.html if it exists ─────────────────────────────
index = Path("index.html")
if index.exists():
    import base64, time
    print("\nUploading index.html to repo root ...")
    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/index.html"

    # Get existing SHA if file already in repo
    existing = requests.get(api_url, headers=headers)
    sha = existing.json().get("sha") if existing.status_code == 200 else None

    content = base64.b64encode(index.read_bytes()).decode()
    put = requests.put(api_url, headers=headers, json={
        "message": "[SPARK] Initial portal publish",
        "content": content,
        "branch":  BRANCH,
        **({"sha": sha} if sha else {}),
    })
    if put.status_code in (200, 201):
        print("[OK] index.html uploaded successfully")
    else:
        print(f"[WARN] index.html upload failed: {put.status_code}")
else:
    print("[WARN] index.html not found locally — run the pipeline first to generate it")

# ── Summary ────────────────────────────────────────────────────────────────────
page_url = f"https://{owner}.github.io/{repo}/"
print(f"""
Setup complete.

Your portal will be live at:
  {page_url}

Note: GitHub Pages can take 1-2 minutes to build after first setup.

After that, every daily SPARK run will:
  1. Generate a fresh index.html with the new project row
  2. Commit it to the repo root via the Publisher Agent
  3. GitHub Pages will automatically redeploy within ~30 seconds
""")