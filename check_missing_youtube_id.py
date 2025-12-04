"""
Fetch all video IDs from the three Thoibao YouTube channels, write them to a
text file, then compare them with the IDs present in exported_data/thoibao/article_videos.csv.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Iterable, Set, Tuple
from urllib.parse import parse_qs, urlparse

try:
    from yt_dlp import YoutubeDL
except ImportError as exc:  # pragma: no cover - dependency check
    raise SystemExit(
        "yt-dlp is required. Install it with: pip install yt-dlp"
    ) from exc

CHANNELS = {
    "thoibao-de": "https://www.youtube.com/@thoibao-de",
    "ThoibaoNews": "https://www.youtube.com/@ThoibaoNews",
    "ThoibaoEU": "https://www.youtube.com/@ThoibaoEU",
}

THIS_DIR = Path(__file__).resolve().parent
ARTICLE_VIDEOS_CSV = THIS_DIR.parent / "thoibao" / "article_videos.csv"
CHANNEL_IDS_TXT = THIS_DIR / "channel_video_ids.txt"


def fetch_channel_video_ids(channel_url: str) -> Set[str]:
    """Return the set of video IDs for the given channel URL."""
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": True,  # only need ids, no recursive downloads
    }
    ids: Set[str] = set()
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(channel_url, download=False)
        entries = info.get("entries") or []
        for entry in entries:
            if not entry:
                continue
            # entry may be a dict or a LazyDict
            vid = entry.get("id") if hasattr(entry, "get") else None
            if vid:
                ids.add(vid)
    return ids


def extract_video_id(url: str) -> str | None:
    """Extract a YouTube video id from common URL formats."""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path

    if "youtube.com" in host:
        if path.startswith("/embed/"):
            return path.split("/")[2].split("?")[0]
        if path.startswith("/shorts/"):
            return path.split("/")[2].split("?")[0]
        if path == "/watch":
            return parse_qs(parsed.query).get("v", [None])[0]
    if "youtu.be" in host:
        slug = path.lstrip("/").split("/")[0]
        if slug:
            return slug

    match = re.search(r"([A-Za-z0-9_-]{11})", url)
    return match.group(1) if match else None


def load_article_video_ids(csv_path: Path) -> Set[str]:
    ids: Set[str] = set()
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            url = row.get("video_path") or ""
            vid = extract_video_id(url)
            if vid:
                ids.add(vid)
    return ids


def write_ids(ids: Iterable[str], path: Path) -> None:
    path.write_text("\n".join(sorted(ids)), encoding="utf-8")


def compare_sets(channel_ids: Set[str], article_ids: Set[str]) -> Tuple[Set[str], Set[str]]:
    """Return (missing_in_article, extra_in_article)."""
    missing = channel_ids - article_ids
    extra = article_ids - channel_ids
    return missing, extra


def main() -> None:
    print("Collecting video IDs from channels...")
    all_channel_ids: Set[str] = set()
    for name, url in CHANNELS.items():
        ids = fetch_channel_video_ids(url)
        all_channel_ids.update(ids)
        print(f"- {name}: {len(ids)} videos")

    write_ids(all_channel_ids, CHANNEL_IDS_TXT)
    print(f"Wrote {len(all_channel_ids)} unique IDs to {CHANNEL_IDS_TXT}")

    if not ARTICLE_VIDEOS_CSV.exists():
        raise SystemExit(f"Missing CSV file: {ARTICLE_VIDEOS_CSV}")
    article_ids = load_article_video_ids(ARTICLE_VIDEOS_CSV)
    print(f"Loaded {len(article_ids)} IDs from {ARTICLE_VIDEOS_CSV}")

    missing, extra = compare_sets(all_channel_ids, article_ids)
    print(f"\nIn channel list but NOT in article_videos.csv: {len(missing)}")
    if missing:
        for vid in sorted(missing):
            print(f"  {vid}")

    print(f"\nIn article_videos.csv but NOT in channel list: {len(extra)}")
    if extra:
        for vid in sorted(extra):
            print(f"  {vid}")


if __name__ == "__main__":
    main()
