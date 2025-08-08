import argparse
import csv
import io
import os
import sys
import subprocess
from pathlib import Path
from typing import List

import urllib.request


DEFAULT_FINVIZ_URL = (
    "https://elite.finviz.com/news_export.ashx?pid=1000773919&auth=1720158e-f218-46ec-8beb-ced231693232"
)

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Finviz news CSV and prepare sentiment columns.")
    parser.add_argument("--rows", type=int, default=None, help="Number of headlines to include (top N rows)")
    parser.add_argument(
        "--scores",
        type=int,
        default=None,
        help="Number of different sentiment scores to include (1 to 5)",
    )
    parser.add_argument(
        "--finviz-url",
        type=str,
        default=os.environ.get("FINVIZ_URL", DEFAULT_FINVIZ_URL),
        help="Finviz CSV export URL",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(os.environ.get("OUTPUT_CSV", "/workspace/output/news_with_sentiment.csv")),
        help="Path to write the augmented CSV",
    )
    return parser.parse_args()


def prompt_if_needed(args: argparse.Namespace) -> argparse.Namespace:
    if args.rows is None:
        while True:
            try:
                rows_str = input("How many headlines should be included? ").strip()
                args.rows = int(rows_str)
                if args.rows <= 0:
                    raise ValueError
                break
            except ValueError:
                print("Please enter a positive integer.")
    if args.scores is None:
        while True:
            try:
                scores_str = input("How many different sentiment scores? (1 to 5) ").strip()
                args.scores = int(scores_str)
                if not (1 <= args.scores <= 5):
                    raise ValueError
                break
            except ValueError:
                print("Please enter an integer from 1 to 5.")
    return args


def fetch_finviz_csv(finviz_url: str) -> List[List[str]]:
    req = urllib.request.Request(
        finviz_url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/csv,application/csv,application/octet-stream,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        csv_bytes = response.read()
    # Finviz returns CSV; decode content
    text_stream = io.StringIO(csv_bytes.decode("utf-8-sig", errors="replace"))
    reader = csv.reader(text_stream)
    rows = [row for row in reader]
    return rows


def limit_rows(rows: List[List[str]], n: int) -> List[List[str]]:
    if not rows:
        return rows
    header, data = rows[0], rows[1:]
    limited = data[:n]
    return [header] + limited


def add_sentiment_columns(rows: List[List[str]], num_scores: int) -> List[List[str]]:
    if not rows:
        return rows
    header = rows[0]

    # Add two columns per requested score: score_i, reason_i
    new_header = header.copy()
    for i in range(1, num_scores + 1):
        new_header.append(f"sentiment_{i}")
        new_header.append(f"sentiment_{i}_reason")

    augmented = [new_header]
    for row in rows[1:]:
        new_row = row.copy()
        # Initialize with blanks for now; downstream steps will fill these
        for _ in range(num_scores):
            new_row.append("")  # sentiment score placeholder
            new_row.append("")  # sentiment reason placeholder
        augmented.append(new_row)

    return augmented


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_csv(rows: List[List[str]], output_path: Path) -> None:
    ensure_parent_dir(output_path)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def try_open_file(output_path: Path) -> None:
    # Best-effort: try xdg-open (Linux), open (macOS), start (Windows)
    try:
        if sys.platform.startswith("linux"):
            subprocess.Popen(["xdg-open", str(output_path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(output_path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        elif os.name == "nt":
            os.startfile(str(output_path))  # type: ignore[attr-defined]
    except Exception:
        # Non-fatal; just print the path
        pass


def main() -> None:
    args = parse_args()
    args = prompt_if_needed(args)

    print("Downloading Finviz news CSV...")
    rows = fetch_finviz_csv(args.finviz_url)
    if not rows:
        print("No data returned from Finviz.")
        sys.exit(1)

    print(f"Trimming to top {args.rows} headline(s)...")
    rows_limited = limit_rows(rows, args.rows)

    print(f"Adding {args.scores} sentiment score column pairs...")
    rows_augmented = add_sentiment_columns(rows_limited, args.scores)

    print(f"Writing CSV to {args.output} ...")
    write_csv(rows_augmented, args.output)

    print("Attempting to open the CSV locally (best-effort)...")
    try_open_file(args.output)

    print("Done.")
    print(f"Saved: {args.output}")


if __name__ == "main__":
    # Fallback if someone runs via python -m
    main()

if __name__ == "__main__":
    main()