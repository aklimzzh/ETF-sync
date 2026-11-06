"""ETF collection entry point for daily, single-day and range workflows."""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from src.exchanges.sse import fetch_sse
from src.exchanges.szse import fetch_szse

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
VALID_EXCHANGES = {"ALL", "SSE", "SZSE"}


def _target_yesterday() -> str:
    return (
        datetime.now(ZoneInfo("Asia/Shanghai")).date()
        - timedelta(days=1)
    ).isoformat()


def _write_csv(records: list[dict], target: str) -> Path:
    path = DATA / "normalized" / target[:4] / f"{target}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "trade_date",
        "exchange",
        "fund_code",
        "fund_name",
        "etf_type",
        "total_share",
        "source",
    ]

    with path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)

    return path


def _health(api_url: str) -> dict:
    response = requests.get(
        f"{api_url.rstrip('/')}/api/v1/health",
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _upload_exchange(
    api_url: str,
    token: str,
    exchange: str,
    target: str,
    records: list[dict],
) -> int:
    batch_size = 250
    batches = 0
    total_records = len(records)

    for offset in range(0, total_records, batch_size):
        batch = records[offset : offset + batch_size]
        is_final_batch = offset + len(batch) >= total_records

        payload = {
            "trade_date": target,
            "exchange": exchange,
            "records": batch,
            "github_run_id": os.getenv(
                "GITHUB_RUN_ID",
                "manual",
            ),
            "github_commit": os.getenv(
                "GITHUB_SHA",
                "",
            )[:40],
            # Canonical API field.
            "finalize": is_final_batch,
            # Retained temporarily for backward compatibility.
            "is_final_batch": is_final_batch,
            "total_record_count": total_records,
        }

        response = requests.post(
            f"{api_url.rstrip('/')}/api/v1/import",
            headers={
                "Authorization": f"Bearer {token}",
            },
            json=payload,
            timeout=90,
        )

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"API import HTTP {response.status_code}: "
                f"{response.text[:1000]}"
            ) from exc

        result = response.json()
        if result.get("success") is not True:
            raise RuntimeError(
                f"API import failed: {result}"
            )

        if bool(result.get("finalize")) != is_final_batch:
            raise RuntimeError(
                "API finalize acknowledgement mismatch: "
                f"sent {is_final_batch}, "
                f"received {result.get('finalize')}"
            )

        if is_final_batch:
            returned_count = result.get("record_count")
            if returned_count != total_records:
                raise RuntimeError(
                    "Final record count mismatch: "
                    f"sent {total_records}, "
                    f"API returned {returned_count}"
                )

        batches += 1

    return batches


def run(
    target: str,
    exchange: str,
    no_upload: bool,
) -> int:
    if exchange not in VALID_EXCHANGES:
        raise ValueError(f"Invalid exchange: {exchange}")

    records_by_exchange: dict[str, list[dict]] = {}
    errors: dict[str, str] = {}
    year = target[:4]

    if exchange in {"ALL", "SSE"}:
        try:
            path = (
                DATA
                / "raw"
                / "SSE"
                / year
                / f"{target}.json"
            )
            rows = fetch_sse(target, path)
            records_by_exchange["SSE"] = rows
            print(f"SSE: {len(rows)} records, PASS")
        except Exception as exc:
            errors["SSE"] = str(exc)
            print(
                f"SSE: FAILED - {exc}",
                file=sys.stderr,
            )

    if exchange in {"ALL", "SZSE"}:
        try:
            path = (
                DATA
                / "raw"
                / "SZSE"
                / year
                / f"{target}.xlsx"
            )
            rows = fetch_szse(target, path)
            records_by_exchange["SZSE"] = rows
            print(f"SZSE: {len(rows)} records, PASS")
        except Exception as exc:
            errors["SZSE"] = str(exc)
            print(
                f"SZSE: FAILED - {exc}",
                file=sys.stderr,
            )

    all_records = [
        row
        for market in ("SSE", "SZSE")
        for row in records_by_exchange.get(market, [])
    ]

    print(f"Target date: {target}")

    if all_records:
        csv_path = _write_csv(all_records, target)
        print(
            f"Normalized CSV: {csv_path} "
            f"({len(all_records)} rows)"
        )

    if not no_upload and records_by_exchange:
        api_url = os.environ.get(
            "ETF_API_URL",
            "",
        ).strip()
        token = os.environ.get(
            "ETF_API_TOKEN",
            "",
        ).strip()

        if not api_url or not token:
            raise RuntimeError(
                "ETF_API_URL and ETF_API_TOKEN "
                "are required for upload"
            )

        print(f"API health: {_health(api_url)}")

        for market, rows in records_by_exchange.items():
            batches = _upload_exchange(
                api_url,
                token,
                market,
                target,
                rows,
            )
            print(
                f"{market}: uploaded in {batches} "
                "batch(es); import finalized"
            )

    if errors:
        print(
            "Partial completion: "
            + "; ".join(
                f"{key}: {value}"
                for key, value in errors.items()
            ),
            file=sys.stderr,
        )
        return 2

    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "YYYY-MM-DD; default is yesterday "
            "in Asia/Shanghai"
        ),
    )
    parser.add_argument(
        "--exchange",
        choices=sorted(VALID_EXCHANGES),
        default="ALL",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Accepted for workflow compatibility",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
    )

    parser.add_argument(
        "--allow-not-ready",
        action="store_true",
        help=(
            "Allow scheduled daily runs to exit cleanly "
            "when exchange data is not ready."
        ),
    )
    
    args = parser.parse_args()
    target = args.date or _target_yesterday()
    date.fromisoformat(target)

    return run(
        target,
        args.exchange,
        args.no_upload,
    )


if __name__ == "__main__":
    raise SystemExit(main())
