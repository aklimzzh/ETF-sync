"""SZSE official XLSX downloader and normalizer."""

from __future__ import annotations

import io
import re
import subprocess
import zipfile
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd

SZSE_URL = "https://www.szse.cn/api/report/ShowReport"
SZSE_REFERER = "https://www.szse.cn/market/fund/volume/etf/index.html"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)
SZSE_SHARE_UNIT_DIVISOR = Decimal("10000")


class SZSEError(RuntimeError):
    pass


def _pick(columns, candidates, exclude=()):
    names = [str(c).strip() for c in columns]
    for candidate in candidates:
        for original, name in zip(columns, names):
            if candidate in name and not any(word in name for word in exclude):
                return original
    raise SZSEError(f"Required SZSE column not found. Columns: {names}")


def _header_row(content: bytes) -> int:
    preview = pd.read_excel(io.BytesIO(content), header=None, nrows=20, engine="openpyxl")
    for index, row in preview.iterrows():
        text = "|".join(str(value) for value in row.tolist())
        if ("代码" in text or "编号" in text) and ("份额" in text or "规模" in text):
            return int(index)
    raise SZSEError("Unable to find SZSE XLSX header row")


def fetch_szse(trade_date: str, raw_path: Path) -> list[dict]:
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    url = (
        f"{SZSE_URL}?SHOWTYPE=xlsx&CATALOGID=scsj_fund_jjgm&TABKEY=tab1"
        f"&txtStart={trade_date}&txtEnd={trade_date}&jjlb=ETF"
    )
    command = [
        "curl", "--fail", "--show-error", "--silent", "--location",
        "--retry", "4", "--retry-delay", "3", "--retry-all-errors",
        "--connect-timeout", "20", "--max-time", "120", "--http1.1",
        "--user-agent", USER_AGENT, "--referer", SZSE_REFERER,
        "--output", str(raw_path), url,
    ]
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise SZSEError(f"SZSE curl failed: {result.stderr.strip()}")
    if not zipfile.is_zipfile(raw_path):
        preview = raw_path.read_bytes()[:300]
        raise SZSEError(f"SZSE response is not a valid XLSX: {preview!r}")

    content = raw_path.read_bytes()
    frame = pd.read_excel(
        io.BytesIO(content), header=_header_row(content), dtype=str, engine="openpyxl"
    ).dropna(how="all")

    date_col = _pick(frame.columns, ("日期",))
    code_col = _pick(frame.columns, ("基金代码", "证券代码", "代码", "编号"))
    name_col = _pick(frame.columns, ("基金简称", "证券简称", "简称", "名称"))
    share_col = _pick(frame.columns, ("基金规模", "总份额", "份额", "规模"), ("变化", "增减"))

    normalized: list[dict] = []
    dates: set[str] = set()
    for _, row in frame.iterrows():
        code = re.sub(r"\.0$", "", str(row.get(code_col, "")).strip())
        name = str(row.get(name_col, "")).strip()
        if not code or code.lower() == "nan" or not name or name.lower() == "nan":
            continue
        try:
            actual_date = pd.to_datetime(row.get(date_col), errors="raise").date().isoformat()
            raw_share = str(row.get(share_col, "")).replace(",", "").strip()
            # Official SZSE XLSX unit is shares; database unit is 10,000 shares.
            total_share = Decimal(raw_share) / SZSE_SHARE_UNIT_DIVISOR
        except (ValueError, TypeError, InvalidOperation) as exc:
            raise SZSEError(f"Invalid SZSE row: {row.to_dict()}") from exc
        dates.add(actual_date)
        normalized.append(
            {
                "trade_date": actual_date,
                "exchange": "SZSE",
                "fund_code": code.zfill(6),
                "fund_name": name,
                "etf_type": None,
                "total_share": str(total_share),
                "source": "SZSE_XLSX",
            }
        )

    if dates != {trade_date}:
        raise SZSEError(f"SZSE date mismatch: expected {trade_date}, received {sorted(dates)}")
    if len(normalized) < 600:
        raise SZSEError(f"Unexpectedly low SZSE record count: {len(normalized)}")
    if len({r['fund_code'] for r in normalized}) != len(normalized):
        raise SZSEError("Duplicate SZSE fund codes detected")
    return normalized
