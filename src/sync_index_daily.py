"""Daily market-index synchronization through AKShare and Serv00 API."""

from __future__ import annotations

import json
import math
import os
import random
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd
import requests


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

INDEX_CONFIG = {
    "SSE": {
        "symbol": "000001",
        "name": "上证指数",
    },
    "SZSE": {
        "symbol": "399001",
        "name": "深证成指",
    },
    "STAR50": {
        "symbol": "000688",
        "name": "科创50",
    },
    "CHINEXT": {
        "symbol": "399006",
        "name": "创业板指",
    },
}

INDEX_ORDER = (
    "SSE",
    "SZSE",
    "STAR50",
    "CHINEXT",
)

COLUMN_MAP = {
    "日期": "trade_date",
    "开盘": "open_price",
    "收盘": "close_price",
    "最高": "high_price",
    "最低": "low_price",
    "成交量": "volume",
    "成交额": "amount",
    "振幅": "amplitude_pct",
    "涨跌幅": "change_pct",
    "涨跌额": "change_amount",
    "换手率": "turnover_pct",
}

REQUIRED_COLUMNS = {
    "trade_date",
    "open_price",
    "close_price",
    "high_price",
    "low_price",
    "volume",
    "amount",
    "amplitude_pct",
    "change_pct",
    "change_amount",
    "turnover_pct",
}

NUMERIC_COLUMNS = (
    "open_price",
    "close_price",
    "high_price",
    "low_price",
    "volume",
    "amount",
    "amplitude_pct",
    "change_pct",
    "change_amount",
    "turnover_pct",
)

FETCH_RETRY_DELAYS = (5, 15, 30)
UPLOAD_RETRY_DELAYS = (5, 15, 30)

TRANSIENT_HTTP_STATUS = {
    408,
    425,
    429,
    500,
    502,
    503,
    504,
}


def _required_environment() -> tuple[str, str]:
    api_url = os.environ.get(
        "ETF_API_URL",
        "",
    ).strip().rstrip("/")

    api_token = os.environ.get(
        "ETF_API_TOKEN",
        "",
    ).strip()

    if not api_url:
        raise RuntimeError(
            "ETF_API_URL environment variable is required"
        )

    if not api_token:
        raise RuntimeError(
            "ETF_API_TOKEN environment variable is required"
        )

    return api_url, api_token


def _shanghai_today() -> date:
    return datetime.now(SHANGHAI_TZ).date()


def _date_range() -> tuple[str, str]:
    """Return the latest seven natural days in AKShare format."""

    today = _shanghai_today()

    start_date = (
        today - timedelta(days=7)
    ).strftime("%Y%m%d")

    end_date = today.strftime("%Y%m%d")

    return start_date, end_date


def _json_response(
    response: requests.Response,
    operation: str,
) -> dict[str, Any]:
    try:
        result = response.json()
    except ValueError as exc:
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


def _fetch_index(
    index_code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    config = INDEX_CONFIG[index_code]

    attempts = len(FETCH_RETRY_DELAYS) + 1

    for attempt in range(1, attempts + 1):
        try:
            print(
                f"{index_code}: fetching "
                f"{config['name']} ({config['symbol']}), "
                f"{start_date} to {end_date}, "
                f"attempt {attempt}/{attempts}"
            )

            frame = ak.index_zh_a_hist(
                symbol=config["symbol"],
                period="daily",
                start_date=start_date,
                end_date=end_date,
            )

            if not isinstance(frame, pd.DataFrame):
                raise RuntimeError(
                    "AKShare did not return a DataFrame"
                )

            print(
                f"{index_code}: AKShare returned "
                f"{len(frame)} row(s)"
            )

            return frame

        except Exception as exc:
            print(
                f"{index_code}: fetch attempt "
                f"{attempt}/{attempts} failed: {exc}",
                file=sys.stderr,
            )

            if attempt < attempts:
                base_delay = FETCH_RETRY_DELAYS[
                    attempt - 1
                ]

                delay = base_delay + random.uniform(
                    0,
                    2,
                )

                print(
                    f"{index_code}: retrying fetch "
                    f"in {delay:.1f}s..."
                )

                time.sleep(delay)

    raise RuntimeError(
        f"{index_code}: AKShare fetch failed "
        f"after {attempts} attempts"
    )


def _safe_value(value: Any) -> Any:
    if value is None:
        return None

    if pd.isna(value):
        return None

    if isinstance(
        value,
        (pd.Timestamp, datetime, date),
    ):
        return value.strftime("%Y-%m-%d")

    if hasattr(value, "item"):
        value = value.item()

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None

        return float(value)

    if isinstance(value, int):
        return int(value)

    return value


def _prepare_records(
    frame: pd.DataFrame,
    index_code: str,
) -> list[dict[str, Any]]:
    if frame.empty:
        return []

    config = INDEX_CONFIG[index_code]

    normalized = frame.rename(
        columns=COLUMN_MAP
    ).copy()

    missing_columns = sorted(
        REQUIRED_COLUMNS - set(normalized.columns)
    )

    if missing_columns:
        raise RuntimeError(
            f"{index_code}: missing AKShare columns: "
            f"{missing_columns}"
        )

    normalized = normalized[
        [
            "trade_date",
            "open_price",
            "close_price",
            "high_price",
            "low_price",
            "volume",
            "amount",
            "amplitude_pct",
            "change_pct",
            "change_amount",
            "turnover_pct",
        ]
    ].copy()

    normalized["trade_date"] = pd.to_datetime(
        normalized["trade_date"],
        errors="coerce",
    )

    for column in NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(
            normalized[column],
            errors="coerce",
        )

    invalid_date_count = int(
        normalized["trade_date"].isna().sum()
    )

    invalid_close_count = int(
        normalized["close_price"].isna().sum()
    )

    if invalid_date_count:
        print(
            f"{index_code}: dropping "
            f"{invalid_date_count} invalid date row(s)",
            file=sys.stderr,
        )

    if invalid_close_count:
        print(
            f"{index_code}: dropping "
            f"{invalid_close_count} row(s) "
            "without close price",
            file=sys.stderr,
        )

    normalized = normalized.dropna(
        subset=[
            "trade_date",
            "close_price",
        ]
    )

    normalized = normalized[
        normalized["close_price"] >= 0
    ]

    normalized = normalized[
        normalized["volume"].isna()
        | (normalized["volume"] >= 0)
    ]

    normalized = normalized[
        normalized["amount"].isna()
        | (normalized["amount"] >= 0)
    ]

    invalid_high_low = (
        normalized["high_price"].notna()
        & normalized["low_price"].notna()
        & (
            normalized["high_price"]
            < normalized["low_price"]
        )
    )

    if invalid_high_low.any():
        count = int(invalid_high_low.sum())

        print(
            f"{index_code}: dropping {count} row(s) "
            "where high_price < low_price",
            file=sys.stderr,
        )

        normalized = normalized[
            ~invalid_high_low
        ]

    normalized = normalized.sort_values(
        "trade_date"
    )

    duplicate_count = int(
        normalized.duplicated(
            subset=["trade_date"],
            keep="last",
        ).sum()
    )

    if duplicate_count:
        print(
            f"{index_code}: removing "
            f"{duplicate_count} duplicate date row(s)"
        )

    normalized = normalized.drop_duplicates(
        subset=["trade_date"],
        keep="last",
    )

    records: list[dict[str, Any]] = []

    for _, row in normalized.iterrows():
        volume_value = row.get("volume")

        record = {
            "index_code": index_code,
            "index_symbol": config["symbol"],
            "index_name": config["name"],
            "trade_date": (
                row["trade_date"].strftime("%Y-%m-%d")
            ),
            "open_price": _safe_value(
                row.get("open_price")
            ),
            "high_price": _safe_value(
                row.get("high_price")
            ),
            "low_price": _safe_value(
                row.get("low_price")
            ),
            "close_price": _safe_value(
                row.get("close_price")
            ),
            "volume": (
                None
                if pd.isna(volume_value)
                else int(volume_value)
            ),
            "amount": _safe_value(
                row.get("amount")
            ),
            "change_amount": _safe_value(
                row.get("change_amount")
            ),
            "change_pct": _safe_value(
                row.get("change_pct")
            ),
            "amplitude_pct": _safe_value(
                row.get("amplitude_pct")
            ),
            "turnover_pct": _safe_value(
                row.get("turnover_pct")
            ),
            "source": "akshare",
        }

        records.append(record)

    json.dumps(
        records,
        ensure_ascii=False,
        allow_nan=False,
    )

    if records:
        print(
            f"{index_code}: prepared "
            f"{len(records)} valid row(s), "
            f"{records[0]['trade_date']} to "
            f"{records[-1]['trade_date']}"
        )

    return records


def _upload_records(
    *,
    api_url: str,
    token: str,
    index_code: str,
    records: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not records:
        print(
            f"{index_code}: no records to upload"
        )

        return None

    if len(records) > 500:
        raise RuntimeError(
            f"{index_code}: received {len(records)} rows; "
            "the index import API allows at most 500"
        )

    url = (
        f"{api_url}"
        "/api/v1/index/import"
    )

    payload = {
        "source": "akshare",
        "records": records,
    }

    attempts = len(UPLOAD_RETRY_DELAYS) + 1

    for attempt in range(1, attempts + 1):
        try:
            print(
                f"{index_code}: uploading "
                f"{len(records)} row(s), "
                f"attempt {attempt}/{attempts}"
            )

            response = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                json=payload,
                timeout=(20, 120),
            )

            if response.status_code in (
                TRANSIENT_HTTP_STATUS
            ):
                raise requests.HTTPError(
                    "transient HTTP "
                    f"{response.status_code}",
                    response=response,
                )

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                raise RuntimeError(
                    f"{index_code}: import HTTP "
                    f"{response.status_code}: "
                    f"{response.text[:1000]}"
                ) from exc

            result = _json_response(
                response,
                f"{index_code} index import",
            )

            if result.get("success") is not True:
                raise RuntimeError(
                    f"{index_code}: API returned "
                    f"success != true: {result}"
                )

            if result.get("committed") is not True:
                raise RuntimeError(
                    f"{index_code}: API did not confirm "
                    f"database commit: {result}"
                )

            received = result.get("received")
            upserted = result.get("upserted")

            if received != len(records):
                raise RuntimeError(
                    f"{index_code}: API received count "
                    f"mismatch, sent {len(records)}, "
                    f"received {received}"
                )

            if upserted != len(records):
                raise RuntimeError(
                    f"{index_code}: API upsert count "
                    f"mismatch, sent {len(records)}, "
                    f"upserted {upserted}"
                )

            print(
                f"{index_code}: upload PASS, "
                f"{upserted} row(s) upserted"
            )

            return result

        except RuntimeError:
            # Validation or application errors are deterministic.
            # Do not retry them blindly.
            raise

        except requests.RequestException as exc:
            print(
                f"{index_code}: upload attempt "
                f"{attempt}/{attempts} failed: {exc}",
                file=sys.stderr,
            )

            if attempt < attempts:
                base_delay = UPLOAD_RETRY_DELAYS[
                    attempt - 1
                ]

                delay = base_delay + random.uniform(
                    0,
                    2,
                )

                print(
                    f"{index_code}: retrying upload "
                    f"in {delay:.1f}s..."
                )

                time.sleep(delay)

    raise RuntimeError(
        f"{index_code}: upload failed after "
        f"{attempts} attempts"
    )


def _get_status(
    api_url: str,
) -> dict[str, Any]:
    url = (
        f"{api_url}"
        "/api/v1/index/status"
    )

    response = requests.get(
        url,
        headers={
            "Accept": "application/json",
        },
        timeout=(15, 45),
    )

    response.raise_for_status()

    result = _json_response(
        response,
        "Index status",
    )

    if result.get("success") is not True:
        raise RuntimeError(
            f"Index status returned "
            f"success != true: {result}"
        )

    return result


def _verify_status(
    api_url: str,
    uploaded_latest_dates: dict[str, str],
) -> None:
    result = _get_status(api_url)

    rows = result.get("data")

    if not isinstance(rows, list):
        raise RuntimeError(
            "Index status response has no data list"
        )

    status_by_code = {
        row.get("index_code"): row
        for row in rows
        if isinstance(row, dict)
    }

    print()
    print("Database index status:")

    for index_code in INDEX_ORDER:
        status = status_by_code.get(index_code)

        if status is None:
            raise RuntimeError(
                f"{index_code}: missing from "
                "index status response"
            )

        row_count = status.get("row_count")
        first_date = status.get("first_date")
        latest_date = status.get("latest_date")

        print(
            f"  {index_code:8} "
            f"rows={str(row_count):>6} "
            f"first={first_date} "
            f"latest={latest_date}"
        )

        expected_latest = uploaded_latest_dates.get(
            index_code
        )

        if (
            expected_latest
            and latest_date
            and latest_date < expected_latest
        ):
            raise RuntimeError(
                f"{index_code}: database latest date "
                f"{latest_date} is earlier than uploaded "
                f"date {expected_latest}"
            )


def main() -> int:
    try:
        api_url, api_token = (
            _required_environment()
        )

        start_date, end_date = _date_range()

        print(
            "ETF Monitor - Daily Index Sync"
        )

        print(
            "Shanghai time: "
            f"{datetime.now(SHANGHAI_TZ).isoformat()}"
        )

        print(
            f"AKShare range: "
            f"{start_date} to {end_date}"
        )

        failed_indexes: dict[str, str] = {}
        uploaded_latest_dates: dict[str, str] = {}

        for index_code in INDEX_ORDER:
            print()
            print("=" * 64)

            try:
                frame = _fetch_index(
                    index_code=index_code,
                    start_date=start_date,
                    end_date=end_date,
                )

                records = _prepare_records(
                    frame=frame,
                    index_code=index_code,
                )

                if not records:
                    print(
                        f"{index_code}: no market data "
                        "returned for the selected period"
                    )

                    continue

                _upload_records(
                    api_url=api_url,
                    token=api_token,
                    index_code=index_code,
                    records=records,
                )

                uploaded_latest_dates[index_code] = (
                    records[-1]["trade_date"]
                )

            except Exception as exc:
                failed_indexes[index_code] = str(exc)

                print(
                    f"{index_code}: FAILED - {exc}",
                    file=sys.stderr,
                )

        print()
        print("=" * 64)

        try:
            _verify_status(
                api_url=api_url,
                uploaded_latest_dates=(
                    uploaded_latest_dates
                ),
            )

        except Exception as exc:
            failed_indexes["STATUS"] = str(exc)

            print(
                f"STATUS: FAILED - {exc}",
                file=sys.stderr,
            )

        if failed_indexes:
            print()
            print(
                "Index synchronization completed "
                "with failures:",
                file=sys.stderr,
            )

            for item, message in (
                failed_indexes.items()
            ):
                print(
                    f"  {item}: {message}",
                    file=sys.stderr,
                )

            return 1

        print()
        print(
            "All indexes synchronized successfully."
        )

        return 0

    except Exception as exc:
        print(
            f"Index synchronization failed: {exc}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
