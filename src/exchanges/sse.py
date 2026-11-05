from __future__ import annotations

import json
import time
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests

SSE_URL = "https://query.sse.com.cn/commonQuery.do"
SSE_REFERER = "https://www.sse.com.cn/market/funddata/volumn/etfvolumn/"
SSE_SQL_ID = "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L"


def fetch_sse(target_date: date, timeout: tuple[int, int] = (15, 60)) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    params = {
        "sqlId": SSE_SQL_ID,
        "isPagination": "true",
        "pageHelp.pageSize": "2000",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.endPage": "1",
        "pageHelp.cacheSize": "1",
        "STAT_DATE": target_date.isoformat(),
        "_": str(int(time.time() * 1000)),
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130 Safari/537.36",
        "Referer": SSE_REFERER,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    response = requests.get(SSE_URL, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    if "json" not in response.headers.get("Content-Type", "").lower():
        raise RuntimeError(f"SSE returned non-JSON content: {response.headers.get('Content-Type')}")
    payload = response.json()
    records = payload.get("result")
    if not isinstance(records, list):
        raise RuntimeError("SSE response does not contain a result list")
    return payload, records


def normalize_sse(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records:
        try:
            share = Decimal(str(row["TOT_VOL"]).replace(",", "").strip())
        except (KeyError, InvalidOperation) as exc:
            raise ValueError(f"Invalid SSE total share row: {row}") from exc
        normalized.append(
            {
                "trade_date": str(row.get("STAT_DATE", "")).strip(),
                "exchange": "SSE",
                "fund_code": str(row.get("SEC_CODE", "")).strip(),
                "fund_name": str(row.get("SEC_NAME", "")).strip(),
                "etf_type": str(row.get("ETF_TYPE", "")).strip() or None,
                "total_share": str(share),
                "source": "SSE_API",
            }
        )
    return normalized


def save_sse_raw(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
