"""
pipeline/github_sync.py — Download raw data files from GitHub releases.

The upstream repo (ahmerhkhan/psx-intraday) publishes daily .db files as
GitHub Release assets tagged `data-YYYY-MM-DD`.
"""

import logging
import os
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

import requests

log = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"


def _headers(token: Optional[str] = None) -> dict:
    h = {"Accept": "application/vnd.github+json"}
    if token:
        h["Authorization"] = f"token {token}"
    return h


def list_recent_releases(
    repo: str,
    token: Optional[str] = None,
    limit: int = 30,
) -> List[dict]:
    """
    Fetch the most recent releases from a GitHub repo.
    """
    url = f"{GITHUB_API}/repos/{repo}/releases"
    releases = []
    page = 1

    while len(releases) < limit:
        resp = requests.get(
            url,
            headers=_headers(token),
            params={"per_page": min(30, limit - len(releases)), "page": page},
        )
        if resp.status_code == 404:
            log.warning("Repo %s not found or no releases", repo)
            return []
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        releases.extend(data)
        page += 1

    log.info("Fetched %d releases from %s", len(releases), repo)
    return releases[:limit]


def download_release_asset(
    repo: str,
    tag: str,
    dest_dir: Path,
    token: Optional[str] = None,
) -> List[Path]:
    """
    Download all .db assets from a specific release tag.
    Returns list of downloaded file paths.
    """
    url = f"{GITHUB_API}/repos/{repo}/releases/tags/{tag}"
    resp = requests.get(url, headers=_headers(token))
    if resp.status_code == 404:
        log.warning("Release %s not found in %s", tag, repo)
        return []
    resp.raise_for_status()

    release = resp.json()
    assets = release.get("assets", [])
    downloaded = []

    dest_dir.mkdir(parents=True, exist_ok=True)

    for asset in assets:
        name = asset["name"]
        if not name.endswith(".db"):
            continue

        out_path = dest_dir / name
        if out_path.exists():
            log.debug("Already have %s, skipping", name)
            downloaded.append(out_path)
            continue

        dl_url = asset["browser_download_url"]
        log.info("Downloading %s from release %s", name, tag)

        dl_resp = requests.get(dl_url, headers=_headers(token), stream=True)
        dl_resp.raise_for_status()

        with open(out_path, "wb") as f:
            for chunk in dl_resp.iter_content(chunk_size=8192):
                f.write(chunk)

        downloaded.append(out_path)
        log.info("  → saved %s (%.1f MB)", out_path, out_path.stat().st_size / 1e6)

    return downloaded


def download_artifacts_for_run(
    repo: str,
    run_id: int,
    dest_dir: Path,
    token: Optional[str] = None,
) -> List[Path]:
    """
    Download artifact ZIP files from a specific workflow run, extract .db files.
    Requires a token with actions:read permission.
    """
    if not token:
        log.warning("GITHUB_TOKEN not set — cannot download artifacts (only available via API with auth)")
        return []

    url = f"{GITHUB_API}/repos/{repo}/actions/runs/{run_id}/artifacts"
    resp = requests.get(url, headers=_headers(token))
    resp.raise_for_status()

    artifacts = resp.json().get("artifacts", [])
    downloaded = []
    dest_dir.mkdir(parents=True, exist_ok=True)

    for art in artifacts:
        if art.get("expired"):
            continue

        dl_url = art["archive_download_url"]
        zip_path = dest_dir / f"{run_id}_{art['name']}.zip"

        if zip_path.exists():
            continue

        dl_resp = requests.get(dl_url, headers=_headers(token))
        if dl_resp.status_code != 200:
            log.error("Failed to download artifact %s", art["name"])
            continue

        with open(zip_path, "wb") as f:
            f.write(dl_resp.content)

        # Extract .db files
        try:
            with zipfile.ZipFile(zip_path) as z:
                for member in z.namelist():
                    if member.endswith(".db"):
                        out_path = dest_dir / os.path.basename(member)
                        if not out_path.exists():
                            with open(out_path, "wb") as f:
                                f.write(z.read(member))
                            downloaded.append(out_path)
        except Exception as e:
            log.error("Error extracting %s: %s", zip_path, e)

    return downloaded


def sync_date(
    target_date: date,
    repo: str,
    dest_dir: Path,
    token: Optional[str] = None,
) -> List[Path]:
    """
    Download the .db file for a specific date from GitHub.
    Tries release first (tag = data-YYYY-MM-DD), then falls back to artifacts.
    """
    tag = f"data-{target_date.isoformat()}"
    log.info("Syncing data for %s from GitHub (%s)", target_date, repo)

    # Try release first
    files = download_release_asset(repo, tag, dest_dir, token)
    if files:
        return files

    log.info("No release found for %s, trying workflow artifacts...", tag)

    # Try artifacts (requires token)
    if token:
        url = f"{GITHUB_API}/repos/{repo}/actions/runs"
        resp = requests.get(
            url,
            headers=_headers(token),
            params={"per_page": 5, "status": "success"},
        )
        if resp.status_code == 200:
            runs = resp.json().get("workflow_runs", [])
            for run in runs:
                created = run["created_at"][:10]  # YYYY-MM-DD
                if created == target_date.isoformat():
                    files = download_artifacts_for_run(repo, run["id"], dest_dir, token)
                    if files:
                        return files

    log.warning("No data found for %s on GitHub", target_date)
    return []


def sync_recent(
    repo: str,
    dest_dir: Path,
    days_back: int = 7,
    token: Optional[str] = None,
) -> List[Path]:
    """
    Download the last `days_back` days of data from GitHub.
    """
    today = date.today()
    all_files = []

    for i in range(days_back):
        d = today - timedelta(days=i)
        files = sync_date(d, repo, dest_dir, token)
        all_files.extend(files)

    log.info("Synced %d files for the last %d days", len(all_files), days_back)
    return all_files
