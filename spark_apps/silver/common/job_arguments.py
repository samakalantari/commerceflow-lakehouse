"""Shared command-line contract for transactional Silver source reads."""

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Optional

from spark_apps.silver.common.bronze_reader import parse_ingested_date


@dataclass(frozen=True)
class SourceSelection:
    mode: str
    ingested_date: Optional[date]


def get_source_selection() -> SourceSelection:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ingested-date",
        type=parse_ingested_date,
        help="UTC Bronze ingestion date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--source-mode",
        choices=("daily", "historical"),
        default="daily",
        help="Read one new_data day (daily) or every historical_v1 partition.",
    )
    args = parser.parse_args()

    if args.source_mode == "daily" and args.ingested_date is None:
        parser.error("--ingested-date is required when --source-mode=daily")

    return SourceSelection(mode=args.source_mode, ingested_date=args.ingested_date)
