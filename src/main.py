"""ETF collection entry point for daily, single-day, and range workflows."""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from src.exchanges.sse import fetch_sse
from src.exchanges.szse import fetch_szse

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
VALID_EXCHANGES = {"ALL", "SSE", "SZSE"}

HEALTH_RETRY_DELAYS = (5, 15, 30)
UPLOAD_RETRY_DELAYS = (5, 15, 30)
TRANSIENT_HTTP_STATUS = {408, 425, 429, 500, 502, 503, 504}


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
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)
    return path


def _json_response(response: requests.Response, operation: str) -> dict[str, Any]:
    try:
        result = response.json()
    except (ValueError, json.JSONDecodeError) as exc:
        preview = response.text[:1000]
        raise RuntimeError(
            f"{operation} returned invalid JSON: {preview}"
        ) from exc

    if not isinstance(result, dict):
        raise RuntimeError(
            f"{operation} returned unexpected JSON type: "
            f"{type(result).__name__}"
        )
    return result


def _health(api_url: str) -> dict[str, Any] | None:
    """Check API health with retry.

    Health is diagnostic only. If all attempts fail, imports still proceed because
    POST /api/v1/import is the authoritative operation and has its own retries.
    """
    url = f"{api_url.rstrip('/')}/api/v1/health"
    attempts = len(HEALTH_RETRY_DELAYS) + 1

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                url,
                timeout=(15, 45),
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            result = _json_response(response, "API health")

            if result.get("success") is not True:
                raise RuntimeError(
                    f"API health returned success != true: {result}"
                )
            return result

        except (requests.RequestException, RuntimeError) as exc:
            print(
                f"API health attempt {attempt}/{attempts} failed: {exc}",
                file=sys.stderr,
            )
            if attempt < attempts:
                delay = HEALTH_RETRY_DELAYS[attempt - 1]
                print(f"Retrying API health in {delay}s...")
                time.sleep(delay)

    print(
        "WARNING: API health check failed after retries; "
        "continuing with import API.",
        file=sys.stderr,
    )
    return None


def _post_import_batch(
    *,
    url: str,
    token: str,
    payload: dict[str, Any],
    exchange: str,
    batch_number: int,
    total_batches: int,
) -> dict[str, Any]:
    attempts = len(UPLOAD_RETRY_DELAYS) + 1

    for attempt in range(1, attempts + 1):
        try:
            response = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                json=payload,
                timeout=(20, 120),
            )

            if response.status_code in TRANSIENT_HTTP_STATUS:
                raise requests.HTTPError(
                    f"transient HTTP {response.status_code}",
                    response=response,
                )

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                raise RuntimeError(
                    f"API import HTTP {response.status_code}: "
                    f"{response.text[:1000]}"
                ) from exc

            result = _json_response(response, "API import")
            if result.get("success") is not True:
                raise RuntimeError(f"API import failed: {result}")
            return result

        except RuntimeError:
            # Application/protocol errors are deterministic and should not be
            # retried blindly.
            raise
        except requests.RequestException as exc:
            print(
                f"{exchange} upload batch {batch_number}/{total_batches} "
                f"attempt {attempt}/{attempts} failed: {exc}",
                file=sys.stderr,
            )
            if attempt < attempts:
                base_delay = UPLOAD_RETRY_DELAYS[attempt - 1]
                # Small jitter avoids repeated synchronization with a busy edge.
                delay = base_delay + random.uniform(0, 2)
                print(f"Retrying upload in {delay:.1f}s...")
                time.sleep(delay)

    raise RuntimeError(
        f"{exchange} upload batch {batch_number}/{total_batches} "
        "failed after retries"
    )


def _upload_exchange(
    api_url: str,
    token: str,
    exchange: str,
    target: str,
    records: list[dict],
) -> int:
    batch_size = 250
    total_records = len(records)
    total_batches = (total_records + batch_size - 1) // batch_size
    import_url = f"{api_url.rstrip('/')}/api/v1/import"

    for batch_index, offset in enumerate(
        range(0, total_records, batch_size),
        start=1,
    ):
        batch = records[offset : offset + batch_size]
        is_final_batch = offset + len(batch) >= total_records
        payload = {
            "trade_date": target,
            "exchange": exchange,
            "records": batch,
            "github_run_id": os.getenv("GITHUB_RUN_ID", "manual"),
            "github_commit": os.getenv("GITHUB_SHA", "")[:40],
            "finalize": is_final_batch,
            "is_final_batch": is_final_batch,
            "total_record_count": total_records,
        }

        result = _post_import_batch(
            url=import_url,
            token=token,
            payload=payload,
            exchange=exchange,
            batch_number=batch_index,
            total_batches=total_batches,
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

        print(
            f"{exchange}: upload batch {batch_index}/{total_batches} PASS"
        )

    return total_batches


def _is_not_ready_error(message: str) -> bool:
    """Recognize an exchange response that means the requested date is absent.

    HTTP 403 is deliberately not classified as not-ready because it can indicate
    exchange-side blocking or a request problem and should remain visible.
    """
    text = message.lower()
    markers = (
        "received []",
        "returned zero records",
        "no records",
        "no data",
        "date mismatch",
        "target date not available",
    )
    return any(marker in text for marker in markers) and "403" not in text


def run(
    target: str,
    exchange: str,
    no_upload: bool,
    allow_not_ready: bool = False,
) -> int:
    if exchange not in VALID_EXCHANGES:
        raise ValueError(f"Invalid exchange: {exchange}")

    records_by_exchange: dict[str, list[dict]] = {}
    errors: dict[str, str] = {}
    year = target[:4]

    if exchange in {"ALL", "SSE"}:
        try:
            path = DATA / "raw" / "SSE" / year / f"{target}.json"
            rows = fetch_sse(target, path)
            records_by_exchange["SSE"] = rows
            print(f"SSE: {len(rows)} records, PASS")
        except Exception as exc:
            errors["SSE"] = str(exc)
            print(f"SSE: FAILED - {exc}", file=sys.stderr)

    if exchange in {"ALL", "SZSE"}:
        try:
            path = DATA / "raw" / "SZSE" / year / f"{target}.xlsx"
            rows = fetch_szse(target, path)
            records_by_exchange["SZSE"] = rows
            print(f"SZSE: {len(rows)} records, PASS")
        except Exception as exc:
            errors["SZSE"] = str(exc)
            print(f"SZSE: FAILED - {exc}", file=sys.stderr)

    all_records = [
        row
        for market in ("SSE", "SZSE")
        for row in records_by_exchange.get(market, [])
    ]

    print(f"Target date: {target}")

    if all_records:
        csv_path = _write_csv(all_records, target)
        print(f"Normalized CSV: {csv_path} ({len(all_records)} rows)")

    if not no_upload and records_by_exchange:
        api_url = os.environ.get("ETF_API_URL", "").strip()
        token = os.environ.get("ETF_API_TOKEN", "").strip()
        if not api_url or not token:
            raise RuntimeError(
                "ETF_API_URL and ETF_API_TOKEN are required for upload"
            )

        health = _health(api_url)
        if health is not None:
            print(f"API health: {health}")

        for market, rows in records_by_exchange.items():
            batches = _upload_exchange(
                api_url,
                token,
                market,
                target,
                rows,
            )
            print(
                f"{market}: uploaded in {batches} batch(es); "
                "import finalized"
            )

    if errors:
        not_ready = {
            market: message
            for market, message in errors.items()
            if _is_not_ready_error(message)
        }
        hard_errors = {
            market: message
            for market, message in errors.items()
            if market not in not_ready
        }

        print(
            "Partial completion: "
            + "; ".join(
                f"{key}: {value}" for key, value in errors.items()
            ),
            file=sys.stderr,
        )

        if allow_not_ready and not hard_errors:
            print(
                "Requested exchange data is not ready yet; "
                "scheduled compensation run may retry later."
            )
            return 0

        return 2

    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=None,
        help="YYYY-MM-DD; default is yesterday in Asia/Shanghai",
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
    parser.add_argument("--no-upload", action="store_true")
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
        allow_not_ready=args.allow_not_ready,
    )


if __name__ == "__main__":
    raise SystemExit(main())
