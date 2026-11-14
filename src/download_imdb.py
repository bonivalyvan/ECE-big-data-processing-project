"""Download the official IMDb non-commercial TSV files."""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path
from urllib.request import urlopen


TABLES = (
    "title.akas",
    "title.basics",
    "title.crew",
    "title.episode",
    "title.principals",
    "title.ratings",
    "name.basics",
)
BASE_URL = "https://datasets.imdbws.com"


def download_table(table: str, output_dir: Path, force: bool = False) -> Path:
    """Download one compressed IMDb table, replacing it only after completion."""
    destination = output_dir / f"{table}.tsv.gz"
    if destination.exists() and not force:
        print(f"Keeping existing file: {destination}")
        return destination

    output_dir.mkdir(parents=True, exist_ok=True)
    url = f"{BASE_URL}/{table}.tsv.gz"
    temporary_path: Path | None = None
    try:
        with urlopen(url, timeout=60) as response:
            with tempfile.NamedTemporaryFile(
                dir=output_dir, prefix=f".{table}.", suffix=".part", delete=False
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    temporary_file.write(chunk)
        os.replace(temporary_path, destination)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()
    print(f"Downloaded {url} -> {destination}")
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=TABLES,
        default=list(TABLES),
        help="IMDb tables to download (all seven by default).",
    )
    parser.add_argument(
        "--confirm-large-download",
        action="store_true",
        help="Acknowledge that the complete IMDb TSV files can require substantial disk space.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace existing files after each download completes successfully.",
    )
    args = parser.parse_args()

    if len(args.tables) > 1 and not args.confirm_large_download:
        parser.error(
            "Downloading multiple full IMDb tables can require substantial disk "
            "space; pass --confirm-large-download or select one table."
        )
    for table in args.tables:
        download_table(table, args.output_dir, force=args.force)


if __name__ == "__main__":
    main()
