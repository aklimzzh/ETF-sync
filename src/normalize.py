from __future__ import annotations

from pathlib import Path

import pandas as pd

COLUMNS = ["trade_date", "exchange", "fund_code", "fund_name", "etf_type", "total_share", "source"]


def merge_records(*groups: list[dict]) -> list[dict]:
    result = [row for group in groups for row in group]
    result.sort(key=lambda row: (row["exchange"], row["fund_code"]))
    return result


def save_normalized_csv(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(records, columns=COLUMNS)
    frame["fund_code"] = frame["fund_code"].astype(str).str.zfill(6)
    frame.to_csv(path, index=False, encoding="utf-8-sig")
