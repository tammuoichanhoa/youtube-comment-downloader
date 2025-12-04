#!/usr/bin/env python3
"""Download YouTube comments listed in article_videos.csv using youtube-comment-downloader."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

try:
    from youtube_comment_downloader import (
        SORT_BY_POPULAR,
        SORT_BY_RECENT,
        YoutubeCommentDownloader,
    )
except Exception as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "Missing dependency youtube-comment-downloader. "
        "Install with: pip install youtube-comment-downloader",
    ) from exc

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ARTICLE_VIDEOS = BASE_DIR / "thoibao" / "article_videos.csv"
DEFAULT_ARTICLES = BASE_DIR / "thoibao" / "articles.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "youtube_comments"
DEFAULT_SORT = "recent"
DEFAULT_SLEEP = 0.1

YOUTUBE_ID_RE = re.compile(
    r"(?:youtu\.be/|youtube\.com/(?:watch\?v=|embed/|shorts/|v/|live/))"
    r"([A-Za-z0-9_-]{6,})",
    re.IGNORECASE,
)


@dataclass
class CommentJob:
    article_id: str
    article_title: str | None
    sequence: int
    video_id: str
    url: str


def configure_csv_field_limit(min_limit: int = 100_000_000) -> None:
    """Increase csv.field_size_limit to avoid errors on large rows."""
    try:
        current = csv.field_size_limit()
    except Exception:
        current = 0

    if current >= min_limit:
        return

    new_limit = min(int(1e9), sys.maxsize)
    while new_limit >= min_limit:
        try:
            csv.field_size_limit(new_limit)
            return
        except (OverflowError, ValueError):
            new_limit //= 2


def extract_youtube_id(url: str) -> str | None:
    match = YOUTUBE_ID_RE.search(url)
    return match.group(1) if match else None


def normalize_youtube_url(raw: str) -> str | None:
    candidate = (raw or "").strip()
    if not candidate:
        return None
    if candidate.startswith("//"):
        candidate = "https:" + candidate
    video_id = extract_youtube_id(candidate)
    if video_id:
        return f"https://www.youtube.com/watch?v={video_id}"
    if candidate.lower().startswith("http"):
        return candidate
    if re.fullmatch(r"[A-Za-z0-9_-]{6,}", candidate):
        return f"https://www.youtube.com/watch?v={candidate}"
    return None


def sanitize_component(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", value)
    safe = safe.strip("._")
    return safe or "video"

def read_article_titles(articles_path: Path) -> dict[str, str]:
    titles: dict[str, str] = {}
    if not articles_path.is_file():
        print(f"[warn] Không tìm thấy articles.csv tại {articles_path}, bỏ qua map title", file=sys.stderr)
        return titles

    with articles_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            article_id = (row.get("id") or "").strip()
            title = (row.get("title") or "").strip()
            if article_id and title:
                titles[article_id] = title

    return titles


def read_jobs(
    csv_path: Path,
    limit: int | None,
    article_titles: dict[str, str],
) -> List[CommentJob]:
    configure_csv_field_limit()

    jobs: List[CommentJob] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            article_id = (row.get("article_id") or "").strip()
            raw_video = (row.get("video_path") or "").strip()
            if not article_id or not raw_video:
                continue

            url = normalize_youtube_url(raw_video)
            if not url:
                print(
                    f"[warn] Bỏ qua URL không hợp lệ cho article_id={article_id}: {raw_video}",
                    file=sys.stderr,
                )
                continue

            video_id = extract_youtube_id(url)
            if not video_id:
                print(
                    f"[warn] Không lấy được video_id cho article_id={article_id}: {url}",
                    file=sys.stderr,
                )
                continue

            try:
                sequence = int(row.get("sequence_number", 1))
            except (TypeError, ValueError):
                sequence = 1

            jobs.append(
                CommentJob(
                    article_id=article_id,
                    article_title=article_titles.get(article_id),
                    sequence=sequence,
                    video_id=video_id,
                    url=url,
                ),
            )
            if limit and len(jobs) >= limit:
                break

    return jobs


def build_output_path(job: CommentJob, output_dir: Path) -> Path:
    name = f"{sanitize_component(job.article_id)}_{job.sequence:02d}_{job.video_id}.jsonl"
    return output_dir / name


def choose_sort(sort_label: str) -> str:
    label = sort_label.lower()
    if label in {"recent", "newest"}:
        return SORT_BY_RECENT
    if label in {"popular", "top"}:
        return SORT_BY_POPULAR
    raise ValueError(f"sort_by phải là 'recent' hoặc 'popular', nhận được: {sort_label}")


def write_comments(
    downloader: YoutubeCommentDownloader,
    job: CommentJob,
    output_path: Path,
    sort_by: str,
    language: str | None,
    sleep: float,
    max_comments: int | None,
    static_title: str | None = None,
) -> int | None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        comments: Iterable[dict] = downloader.get_comments_from_url(
            job.url,
            sort_by=sort_by,
            language=language,
            sleep=sleep,
        )
    except Exception as exc:
        print(
            f"[error] Không thể khởi tạo downloader cho {job.url}: {exc}",
            file=sys.stderr,
        )
        return None

    written = 0
    try:
        with output_path.open("w", encoding="utf-8") as handle:
            for comment in comments:
                if static_title:
                    comment["video_title"] = static_title
                json.dump(comment, handle, ensure_ascii=False)
                handle.write("\n")
                written += 1
                if max_comments and written >= max_comments:
                    break
    except Exception as exc:
        print(
            f"[error] Lỗi khi ghi comment cho {job.url}: {exc}",
            file=sys.stderr,
        )
        return None

    return written


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download YouTube comments for Thoibao videos using youtube-comment-downloader.",
    )
    parser.add_argument(
        "--article-videos",
        type=Path,
        default=DEFAULT_ARTICLE_VIDEOS,
        help=f"Path tới file article_videos.csv (default: %(default)s)",
    )
    parser.add_argument(
        "--articles",
        type=Path,
        default=DEFAULT_ARTICLES,
        help=f"Path tới file articles.csv để lấy title (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Thư mục lưu comment (.jsonl) (default: %(default)s)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Chỉ xử lý N video đầu tiên (debug).",
    )
    parser.add_argument(
        "--max-comments",
        type=int,
        default=None,
        help="Số comment tối đa mỗi video (mặc định: tải hết).",
    )
    parser.add_argument(
        "--sort-by",
        choices=["recent", "popular"],
        default=DEFAULT_SORT,
        help="Sắp xếp comment theo 'recent' hoặc 'popular' (default: %(default)s).",
    )
    parser.add_argument(
        "--language",
        default=None,
        help="Mã ngôn ngữ (ví dụ: vi). Để mặc định None cho auto.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help="Thời gian sleep giữa các request (default: %(default)s).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Ghi đè nếu file comment đã tồn tại.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    if not args.article_videos.is_file():
        print(f"File not found: {args.article_videos}", file=sys.stderr)
        return 1

    article_titles = read_article_titles(args.articles)
    jobs = read_jobs(args.article_videos, args.limit, article_titles)
    if not jobs:
        print("Không tìm thấy job hợp lệ trong article_videos.csv", file=sys.stderr)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    sort_by = choose_sort(args.sort_by)
    downloader = YoutubeCommentDownloader()

    downloaded = 0
    skipped = 0
    failed = 0

    for job in jobs:
        output_path = build_output_path(job, args.output_dir)
        if output_path.exists() and not args.overwrite:
            skipped += 1
            print(f"[skip] {output_path} đã tồn tại")
            continue

        written = write_comments(
            downloader=downloader,
            job=job,
            output_path=output_path,
            sort_by=sort_by,
            language=args.language,
            sleep=args.sleep,
            max_comments=args.max_comments,
            static_title=job.article_title,
        )
        if written is None:
            failed += 1
            continue

        downloaded += 1
        print(
            f"[ok] {job.url} -> {output_path} ({written} comments)",
        )

    print(
        f"Done. downloaded={downloaded}, skipped_existing={skipped}, failed={failed}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
