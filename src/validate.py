from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation


class DataNotReadyError(RuntimeError):
    pass


def validate_records(records: list[dict], exchange: str, target_date: date) -> None:
    minimum = 800 if exchange == "SSE" else 600
    if len(records) < minimum:
        raise DataNotReadyError(f"{exchange}: expected at least {minimum} records, received {len(records)}")

    dates = {str(row.get("trade_date", "")) for row in records}
    if dates != {target_date.isoformat()}:
        raise DataNotReadyError(
            f"{exchange}: expected trade date {target_date.isoformat()}, received {sorted(dates)}"
        )

    seen: set[tuple[str, str]] = set()
    required = ("trade_date", "exchange", "fund_code", "fund_name", "total_share", "source")
    for index, row in enumerate(records, start=1):
        missing = [field for field in required if row.get(field) in (None, "")]
        if missing:
            raise ValueError(f"{exchange} row {index}: missing fields {missing}")
        key = (str(row["exchange"]), str(row["fund_code"]))
        if key in seen:
            raise ValueError(f"{exchange}: duplicate key {key}")
        seen.add(key)
        try:
            share = Decimal(str(row["total_share"]))
        except InvalidOperation as exc:
            raise ValueError(f"{exchange} row {index}: invalid total_share") from exc
        if share < 0:
            raise ValueError(f"{exchange} row {index}: negative total_share")
