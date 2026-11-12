"""Daily multi-source index synchronization.

Source priority:
1. Tencent via AKShare stock_zh_index_daily_tx
2. Sina via AKShare stock_zh_index_daily
3. Eastmoney via AKShare index_zh_a_hist

The script normalizes all sources to the existing Serv00 index import API.
"""

from __future__ import annotations

import json
import math
import os
import random
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd
import requests


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
LOOKBACK_DAYS = int(os.getenv("INDEX_LOOKBACK_DAYS", "10"))
STARTUP_JITTER_MAX = int(os.getenv("INDEX_STARTUP_JITTER_MAX", "240"))
BETWEEN_INDEX_MIN = float(os.getenv("INDEX_BETWEEN_INDEX_MIN", "3"))
BETWEEN_INDEX_MAX = float(os.getenv("INDEX_BETWEEN_INDEX_MAX", "8"))
SOURCE_WAIT_SECONDS = float(os.getenv("INDEX_SOURCE_WAIT_SECONDS", "5"))
HTTP_TIMEOUT = (20, 120)

INDEX_CONFIG = {
    "SSE": {
        "name": "上证指数",
        "symbol_tx": "sh000001",
        "symbol_sina": "sh000001",
        "symbol_em": "000001",
    },
    "SZSE": {
        "name": "深证成指",
        "symbol_tx": "sz399001",
        "symbol_sina": "sz399001",
        "symbol_em": "399001",
    },
    "STAR50": {
        "name": "科创50",
        "symbol_tx": "sh000688",
        "symbol_sina": "sh000688",
        "symbol_em": "000688",
    },
    "CHINEXT": {
        "name": "创业板指",
        "symbol_tx": "sz399006",
        "symbol_sina": "sz399006",
        "symbol_em": "399006",
    },
}

INDEX_ORDER = ("SSE", "SZSE", "STAR50", "CHINEXT")
SOURCE_ORDER = ("tencent", "sina", "eastmoney")

CANONICAL_COLUMNS = (
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "amount",
    "change_amount",
    "change_pct",
    "amplitude_pct",
    "turnover_pct",
)

COLUMN_ALIASES = {
    "date": "trade_date",
    "日期": "trade_date",
    "open": "open_price",
    "开盘": "open_price",
    "high": "high_price",
    "最高": "high_price",
    "low": "low_price",
    "最低": "low_price",
    "close": "close_price",
    "收盘": "close_price",
    "volume": "volume",
    "成交量": "volume",
    "vol": "volume",
    "amount": "amount",
    "成交额": "amount",
    "turnover": "turnover_pct",
    "turnover_rate": "turnover_pct",
    "换手率": "turnover_pct",
    "change_amount": "change_amount",
    "涨跌额": "change_amount",
    "change_pct": "change_pct",
    "change_rate": "change_pct",
    "涨跌幅": "change_pct",
    "amplitude": "amplitude_pct",
    "amplitude_pct": "amplitude_pct",
    "振幅": "amplitude_pct",
}


class SourceError(RuntimeError):
    pass


def required_environment() -> tuple[str, str]:
    api_url = os.environ.get("ETF_API_URL", "").strip().rstrip("/")
    api_token = os.environ.get("ETF_API_TOKEN", "").strip()
    if not api_url:
        raise RuntimeError("ETF_API_URL environment variable is required")
    if not api_token:
        raise RuntimeError("ETF_API_TOKEN environment variable is required")
    return api_url, api_token


def shanghai_today() -> date:
    return datetime.now(SHANGHAI_TZ).date()


def request_range() -> tuple[str, str, date, date]:
    end = shanghai_today()
    start = end - timedelta(days=LOOKBACK_DAYS)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), start, end


def safe_json(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.strftime("%Y-%m-%d")
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else float(value)
    if isinstance(value, int):
        return int(value)
    return value


def standardize_source_frame(
    raw: pd.DataFrame,
    *,
    source: str,
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    if not isinstance(raw, pd.DataFrame) or raw.empty:
        raise SourceError(f"{source}: empty DataFrame")

    frame = raw.copy()
    frame.columns = [str(c).strip() for c in frame.columns]
    frame = frame.rename(columns={c: COLUMN_ALIASES.get(c, c) for c in frame.columns})

    required = {"trade_date", "open_price", "high_price", "low_price", "close_price"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise SourceError(f"{source}: missing columns {missing}; actual={list(frame.columns)}")

    for column in CANONICAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA

    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    for column in CANONICAL_COLUMNS:
        if column != "trade_date":
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.dropna(subset=["trade_date", "close_price"])
    frame = frame.sort_values("trade_date").drop_duplicates("trade_date", keep="last")

    # Calculate derived values before filtering, so the first requested day can use
    # the previous trading day's close when the source returned longer history.
    previous_close = frame["close_price"].shift(1)
    calculated_change_amount = frame["close_price"] - previous_close
    calculated_change_pct = calculated_change_amount / previous_close * 100
    calculated_amplitude_pct = (
        (frame["high_price"] - frame["low_price"]) / previous_close * 100
    )

    frame["change_amount"] = frame["change_amount"].fillna(calculated_change_amount)
    frame["change_pct"] = frame["change_pct"].fillna(calculated_change_pct)
    frame["amplitude_pct"] = frame["amplitude_pct"].fillna(calculated_amplitude_pct)

    frame = frame[
        (frame["trade_date"].dt.date >= start_day)
        & (frame["trade_date"].dt.date <= end_day)
    ].copy()

    if frame.empty:
        raise SourceError(f"{source}: no rows in requested date range")

    invalid = (
        (frame["close_price"] < 0)
        | (frame["high_price"] < frame["low_price"])
        | (frame["volume"].notna() & (frame["volume"] < 0))
        | (frame["amount"].notna() & (frame["amount"] < 0))
    )
    if invalid.any():
        raise SourceError(f"{source}: {int(invalid.sum())} invalid market-data row(s)")

    return frame[list(CANONICAL_COLUMNS)].copy()


def fetch_tencent(index_code: str, start_day: date, end_day: date) -> pd.DataFrame:
    symbol = INDEX_CONFIG[index_code]["symbol_tx"]
    raw = ak.stock_zh_index_daily_tx(symbol=symbol)
    return standardize_source_frame(raw, source="tencent", start_day=start_day, end_day=end_day)


def fetch_sina(index_code: str, start_day: date, end_day: date) -> pd.DataFrame:
    symbol = INDEX_CONFIG[index_code]["symbol_sina"]
    raw = ak.stock_zh_index_daily(symbol=symbol)
    return standardize_source_frame(raw, source="sina", start_day=start_day, end_day=end_day)


def fetch_eastmoney(
    index_code: str,
    start_text: str,
    end_text: str,
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    symbol = INDEX_CONFIG[index_code]["symbol_em"]
    raw = ak.index_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_text,
        end_date=end_text,
    )
    return standardize_source_frame(raw, source="eastmoney", start_day=start_day, end_day=end_day)


def fetch_with_fallback(
    index_code: str,
    start_text: str,
    end_text: str,
    start_day: date,
    end_day: date,
) -> tuple[pd.DataFrame, str]:
    errors: list[str] = []

    fetchers: list[tuple[str, Callable[[], pd.DataFrame]]] = [
        ("tencent", lambda: fetch_tencent(index_code, start_day, end_day)),
        ("sina", lambda: fetch_sina(index_code, start_day, end_day)),
        (
            "eastmoney",
            lambda: fetch_eastmoney(
                index_code, start_text, end_text, start_day, end_day
            ),
        ),
    ]

    for source, fetcher in fetchers:
        try:
            print(f"{index_code}: source={source}, fetching...")
            frame = fetcher()
            print(
                f"{index_code}: source={source}, PASS, rows={len(frame)}, "
                f"{frame.iloc[0]['trade_date'].date()} -> {frame.iloc[-1]['trade_date'].date()}"
            )
            return frame, source
        except Exception as exc:
            message = f"{source}: {type(exc).__name__}: {exc}"
            errors.append(message)
            print(f"{index_code}: source={source}, FAILED: {exc}", file=sys.stderr)
            if source != SOURCE_ORDER[-1]:
                time.sleep(SOURCE_WAIT_SECONDS)

    raise RuntimeError(f"{index_code}: all sources failed | " + " | ".join(errors))


def records_from_frame(
    frame: pd.DataFrame,
    index_code: str,
    source: str,
) -> list[dict[str, Any]]:
    config = INDEX_CONFIG[index_code]
    records: list[dict[str, Any]] = []

    for _, row in frame.iterrows():
        volume = row.get("volume")
        records.append(
            {
                "index_code": index_code,
                "index_symbol": config["symbol_em"],
                "index_name": config["name"],
                "trade_date": row["trade_date"].strftime("%Y-%m-%d"),
                "open_price": safe_json(row.get("open_price")),
                "high_price": safe_json(row.get("high_price")),
                "low_price": safe_json(row.get("low_price")),
                "close_price": safe_json(row.get("close_price")),
                "volume": None if pd.isna(volume) else int(volume),
                "amount": safe_json(row.get("amount")),
                "change_amount": safe_json(row.get("change_amount")),
                "change_pct": safe_json(row.get("change_pct")),
                "amplitude_pct": safe_json(row.get("amplitude_pct")),
                "turnover_pct": safe_json(row.get("turnover_pct")),
                "source": source,
            }
        )

    json.dumps(records, ensure_ascii=False, allow_nan=False)
    return records


def upload_records(
    api_url: str,
    token: str,
    index_code: str,
    records: list[dict[str, Any]],
) -> None:
    if not records:
        raise RuntimeError(f"{index_code}: no records prepared")

    response = requests.post(
        f"{api_url}/api/v1/index/import",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "ETF-Monitor-Multi-Source-Index/2.0",
        },
        json={"source": records[0]["source"], "records": records},
        timeout=HTTP_TIMEOUT,
    )

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"{index_code}: import returned invalid JSON, HTTP {response.status_code}: "
            f"{response.text[:800]}"
        ) from exc

    if response.status_code not in {200, 201} or payload.get("success") is not True:
        raise RuntimeError(
            f"{index_code}: import failed, HTTP {response.status_code}: {payload}"
        )

    if payload.get("upserted") != len(records):
        raise RuntimeError(
            f"{index_code}: upsert count mismatch; sent={len(records)}, "
            f"upserted={payload.get('upserted')}"
        )

    print(
        f"{index_code}: uploaded={len(records)}, source={records[0]['source']}, PASS"
    )


def verify_status(api_url: str, expected_dates: dict[str, str]) -> None:
    response = requests.get(
        f"{api_url}/api/v1/index/status",
        headers={"Accept": "application/json"},
        timeout=(15, 45),
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("success") is not True:
        raise RuntimeError(f"status API failed: {payload}")

    rows = {item["index_code"]: item for item in payload.get("data", [])}
    print("\nDatabase status:")
    for code in INDEX_ORDER:
        item = rows.get(code)
        if not item:
            raise RuntimeError(f"{code}: missing from status API")
        latest = item.get("latest_date")
        expected = expected_dates.get(code)
        print(
            f"  {code:8} rows={item.get('row_count'):>6} "
            f"latest={latest} expected>={expected}"
        )
        if expected and (not latest or latest < expected):
            raise RuntimeError(
                f"{code}: database latest date {latest} is earlier than {expected}"
            )


def main() -> int:
    try:
        api_url, api_token = required_environment()
        start_text, end_text, start_day, end_day = request_range()

        print("ETF Monitor - Multi-source Daily Index Sync")
        print(f"Shanghai time: {datetime.now(SHANGHAI_TZ).isoformat()}")
        print(f"Requested range: {start_text} -> {end_text}")
        print("Source priority: tencent -> sina -> eastmoney")

        if STARTUP_JITTER_MAX > 0:
            delay = random.randint(0, STARTUP_JITTER_MAX)
            print(f"Startup jitter: {delay}s")
            time.sleep(delay)

        failures: dict[str, str] = {}
        expected_dates: dict[str, str] = {}
        used_sources: dict[str, str] = {}

        for position, index_code in enumerate(INDEX_ORDER):
            print("\n" + "=" * 64)
            try:
                frame, source = fetch_with_fallback(
                    index_code,
                    start_text,
                    end_text,
                    start_day,
                    end_day,
                )
                records = records_from_frame(frame, index_code, source)
                upload_records(api_url, api_token, index_code, records)
                expected_dates[index_code] = records[-1]["trade_date"]
                used_sources[index_code] = source
            except Exception as exc:
                failures[index_code] = str(exc)
                print(f"{index_code}: FAILED: {exc}", file=sys.stderr)

            if position < len(INDEX_ORDER) - 1:
                delay = random.uniform(BETWEEN_INDEX_MIN, BETWEEN_INDEX_MAX)
                print(f"Waiting {delay:.1f}s before next index...")
                time.sleep(delay)

        try:
            verify_status(api_url, expected_dates)
        except Exception as exc:
            failures["STATUS"] = str(exc)

        print("\nSource summary:")
        for code in INDEX_ORDER:
            print(f"  {code:8} {used_sources.get(code, 'FAILED')}")

        if failures:
            print("\nFailures:", file=sys.stderr)
            for key, value in failures.items():
                print(f"  {key}: {value}", file=sys.stderr)
            return 1

        print("\nAll indexes synchronized successfully.")
        return 0

    except Exception as exc:
        print(f"Index synchronization failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
