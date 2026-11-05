from __future__ import annotations

import io
import re
import zipfile
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

SZSE_URL = "https://www.szse.cn/api/report/ShowReport"
SZSE_REFERER = "https://www.szse.cn/market/fund/volume/etf/index.html"


def download_szse(target_date: date, timeout: tuple[int, int] = (15, 60)) -> bytes:
    params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": "scsj_fund_jjgm",
        "TABKEY": "tab1",
        "txtStart": target_date.isoformat(),
        "txtEnd": target_date.isoformat(),
        "jjlb": "ETF",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130 Safari/537.36",
        "Referer": SZSE_REFERER,
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
    }
    response = requests.get(SZSE_URL, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    content = response.content
    if not zipfile.is_zipfile(io.BytesIO(content)):
        preview = content[:300].decode("utf-8", errors="replace")
        raise RuntimeError(f"SZSE response is not a valid XLSX. Preview: {preview}")
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        if "[Content_Types].xml" not in archive.namelist():
            raise RuntimeError("SZSE XLSX structure is invalid")
    return content


def _clean_header(value: object) -> str:
    return re.sub(r"\s+", "", str(value)).replace("（", "(").replace("）", ")")


def _find_header_row(content: bytes) -> int:
    preview = pd.read_excel(io.BytesIO(content), header=None, nrows=20, engine="openpyxl")
    for idx, row in preview.iterrows():
        joined = "|".join(_clean_header(v) for v in row.tolist() if pd.notna(v))
        if ("代码" in joined or "编号" in joined) and ("简称" in joined or "名称" in joined) and ("份额" in joined or "规模" in joined):
            return int(idx)
    raise RuntimeError("Unable to locate SZSE XLSX header row")


def _pick(columns: Iterable[object], keywords: tuple[str, ...], exclude: tuple[str, ...] = ()) -> object:
    for col in columns:
        name = _clean_header(col)
        if any(key in name for key in keywords) and not any(key in name for key in exclude):
            return col
    raise RuntimeError(f"Unable to find SZSE column matching {keywords}")


def normalize_szse(content: bytes) -> list[dict[str, str | None]]:
    header_row = _find_header_row(content)
    frame = pd.read_excel(io.BytesIO(content), header=header_row, dtype=str, engine="openpyxl")
    frame = frame.dropna(how="all")
    date_col = _pick(frame.columns, ("日期",))
    code_col = _pick(frame.columns, ("基金代码", "证券代码", "代码", "编号"))
    name_col = _pick(frame.columns, ("基金简称", "证券简称", "简称", "名称"))
    share_col = _pick(frame.columns, ("基金规模", "总份额", "份额", "规模"), exclude=("变化", "增减"))

    normalized: list[dict[str, str | None]] = []
    for _, row in frame.iterrows():
        code = str(row.get(code_col, "")).strip()
        name = str(row.get(name_col, "")).strip()
        raw_date = row.get(date_col)
        raw_share = str(row.get(share_col, "")).replace(",", "").strip()
        if not code or code.lower() == "nan" or not name or name.lower() == "nan":
            continue
        code_digits = re.sub(r"\.0$", "", code)
        try:
            trade_date = pd.to_datetime(raw_date, errors="raise").date().isoformat()
            total_share = Decimal(raw_share)
        except (ValueError, TypeError, InvalidOperation) as exc:
            raise ValueError(f"Invalid SZSE row: {row.to_dict()}") from exc
        normalized.append(
            {
                "trade_date": trade_date,
                "exchange": "SZSE",
                "fund_code": code_digits.zfill(6),
                "fund_name": name,
                "etf_type": None,
                "total_share": str(total_share),
                "source": "SZSE_XLSX",
            }
        )
    return normalized


def save_szse_raw(content: bytes, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
