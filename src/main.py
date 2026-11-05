from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

from src.calendar import is_trading_day, previous_calendar_day
from src.exchanges.sse import fetch_sse, normalize_sse, save_sse_raw
from src.exchanges.szse import download_szse, normalize_szse, save_szse_raw
from src.normalize import merge_records, save_normalized_csv
from src.serv00_client import Serv00Client
from src.validate import DataNotReadyError, validate_records

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and import SSE/SZSE ETF share data")
    parser.add_argument("--date", type=date.fromisoformat, help="Target trade date in YYYY-MM-DD")
    parser.add_argument("--force", action="store_true", help="Skip AKShare trading-day gate")
    parser.add_argument("--allow-not-ready", action="store_true", help="Exit 0 if exchange data is not ready")
    parser.add_argument("--no-upload", action="store_true", help="Fetch/archive without calling Serv00")
    parser.add_argument("--batch-size", type=int, default=300)
    return parser.parse_args()


def process(target: date, args: argparse.Namespace) -> int:
    print(f"Target date: {target}")
    if not args.force and not is_trading_day(target):
        print("Status: NOT_TRADING_DAY")
        return 0

    year = str(target.year)
    raw_sse = ROOT / "data" / "raw" / "SSE" / year / f"{target}.json"
    raw_szse = ROOT / "data" / "raw" / "SZSE" / year / f"{target}.xlsx"
    normalized_path = ROOT / "data" / "normalized" / year / f"{target}.csv"

    exchange_records: dict[str, list[dict]] = {}
    errors: list[str] = []

    try:
        payload, rows = fetch_sse(target)
        records = normalize_sse(rows)
        validate_records(records, "SSE", target)
        save_sse_raw(payload, raw_sse)
        exchange_records["SSE"] = records
        print(f"SSE: {len(records)} records, PASS")
    except Exception as exc:
        errors.append(f"SSE: {exc}")
        print(f"SSE: FAILED - {exc}", file=sys.stderr)

    try:
        content = download_szse(target)
        records = normalize_szse(content)
        validate_records(records, "SZSE", target)
        save_szse_raw(content, raw_szse)
        exchange_records["SZSE"] = records
        print(f"SZSE: {len(records)} records, PASS")
    except Exception as exc:
        errors.append(f"SZSE: {exc}")
        print(f"SZSE: FAILED - {exc}", file=sys.stderr)

    if not exchange_records:
        if args.allow_not_ready and all("expected" in error.lower() or "date" in error.lower() for error in errors):
            print("Status: DATA_NOT_READY")
            return 0
        raise RuntimeError("No exchange data passed validation: " + "; ".join(errors))

    merged = merge_records(*exchange_records.values())
    save_normalized_csv(merged, normalized_path)
    print(f"Normalized CSV: {normalized_path} ({len(merged)} rows)")

    if not args.no_upload:
        client = Serv00Client()
        print(f"API health: {client.health()}")
        for exchange, records in exchange_records.items():
            responses = client.import_exchange(
                exchange=exchange,
                trade_date=target,
                records=records,
                batch_size=args.batch_size,
                github_run_id=os.environ.get("GITHUB_RUN_ID"),
                github_commit=os.environ.get("GITHUB_SHA"),
            )
            print(f"{exchange}: uploaded in {len(responses)} batch(es)")

    if errors:
        print("Partial completion: " + "; ".join(errors), file=sys.stderr)
        return 0 if args.allow_not_ready else 2

    print("Status: SUCCESS")
    return 0


def main() -> int:
    args = parse_args()
    target = args.date or previous_calendar_day()
    try:
        return process(target, args)
    except DataNotReadyError as exc:
        print(f"DATA_NOT_READY: {exc}", file=sys.stderr)
        return 0 if args.allow_not_ready else 2
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
