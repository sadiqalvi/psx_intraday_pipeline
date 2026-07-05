import logging
import sys
from pathlib import Path

# Ensure the root project directory is in the PYTHONPATH
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from pipeline.config import load_config
from pipeline.github_sync import list_recent_releases, download_release_asset
from pipeline.run import run_backfill

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("github_full_backfill")


def main():
    print("🚀 Starting FULL GitHub Backfill")
    config = load_config()
    
    repo = config.github_repo
    token = config.github_token
    dest_dir = config.local_historical_dir
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    if not token:
        print("⚠️ WARNING: GH_DATA_TOKEN is not set in your environment variables.")
        print("   Fetching hundreds of releases will likely hit GitHub's unauthenticated API limits (60/hr).")
        print("   It is highly recommended to provide a token before proceeding.")
        print("   Continuing anyway...\n")

    print(f"Fetching all releases from {repo}...")
    # Fetch a high limit to get all historical releases (e.g., up to 365 days of files)
    releases = list_recent_releases(repo, token=token, limit=400)
    
    if not releases:
        print(f"❌ No releases found in {repo}.")
        sys.exit(1)
        
    print(f"Found {len(releases)} releases. Downloading databases...")
    
    downloaded_files = []
    for release in releases:
        tag_name = release.get("tag_name")
        if not tag_name:
            continue
            
        print(f"  → Processing release: {tag_name}")
        files = download_release_asset(repo, tag_name, dest_dir, token=token)
        downloaded_files.extend(files)
        
    print(f"\n✅ Downloaded {len(downloaded_files)} database files into {dest_dir}")
    print("\nStarting the processing & cleaning pipeline...")
    
    # Run the backfill engine (ingest -> clean -> aggregate -> validate -> upload)
    run_backfill(config)
    print("\n🎉 Full GitHub Backfill Complete!")


if __name__ == "__main__":
    main()
