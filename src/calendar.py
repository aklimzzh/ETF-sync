from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def shanghai_today() -> date:
    return datetime.now(SHANGHAI_TZ).date()


def previous_calendar_day(base: date | None = None) -> date:
    return (base or shanghai_today()) - timedelta(days=1)


@lru_cache(maxsize=1)
def trading_days() -> frozenset[date]:
    frame = ak.tool_trade_date_hist_sina()
    if "trade_date" not in frame.columns:
        raise RuntimeError("AKShare trade calendar has no trade_date column")
    values = pd.to_datetime(frame["trade_date"], errors="raise").dt.date
    return frozenset(values.tolist())


def is_trading_day(day: date) -> bool:
    return day in trading_days()
