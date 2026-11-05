"""SSE ETF share downloader.

Uses the system curl binary because query.sse.com.cn may return HTTP 403 to
Python requests even when the same GitHub-hosted runner can access it with curl.
The output is normalized to the platform unit: 10,000 shares.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

SSE_API_URL = "https://query.sse.com.cn/commonQuery.do"
SSE_SQL_ID = "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L"
SSE_REFERER = "https://www.sse.com.cn/market/funddata/volumn/etfvolumn/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)


class SSEError(RuntimeError):
    """Base SSE download/parse error."""


class SSEForbiddenError(SSEError):
    """SSE explicitly rejected the request with HTTP 403."""


def _curl_once(trade_date: str) -> dict:
    """Run one curl request and return the decoded JSON payload."""
    with tempfile.TemporaryDirectory(prefix="sse-etf-") as temp_dir:
        body_path = Path(temp_dir) / "response.json"
        status_path = Path(temp_dir) / "status.txt"

        params = [
            ("sqlId", SSE_SQL_ID),
            ("isPagination", "true"),
            ("pageHelp.pageSize", "2000"),
            ("pageHelp.pageNo", "1"),
            ("pageHelp.beginPage", "1"),
            ("pageHelp.endPage", "1"),
            ("pageHelp.cacheSize", "1"),
            ("STAT_DATE", trade_date),
            ("_", str(int(time.time() * 1000))),
        ]

        command = [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--connect-timeout", "20",
            "--max-time", "90",
            "--http1.1",
            "--compressed",
            "--user-agent", USER_AGENT,
            "--referer", SSE_REFERER,
            "--header", "Accept: application/json, text/javascript, */*; q=0.01",
            "--header", "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8",
            "--header", "Connection: keep-alive",
            "--output", str(body_path),
            "--write-out", "%{http_code}",
            "--get",
        ]
        for key, value in params:
            command.extend(["--data-urlencode", f"{key}={value}"])
        command.append(SSE_API_URL)

        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        status_path.write_text(result.stdout.strip(), encoding="ascii")
        status = result.stdout.strip()

        if result.returncode != 0:
            raise SSEError(
                f"curl failed with exit code {result.returncode}: "
                f"{result.stderr.strip()}"
            )
        if status == "403":
            raise SSEForbiddenError("SSE returned HTTP 403 Forbidden")
        if status != "200":
            preview = body_path.read_text(encoding="utf-8", errors="replace")[:300]
            raise SSEError(f"SSE returned HTTP {status}: {preview}")

        try:
            return json.loads(body_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            preview = body_path.read_text(encoding="utf-8", errors="replace")[:500]
            raise SSEError(f"SSE response is not valid JSON: {preview}") from exc


def fetch_sse(trade_date: str, raw_path: Path) -> list[dict]:
    """Download, validate, archive and normalize one SSE trading day.

    A 403 gets one controlled retry only. Repeated rapid 403 retries tend to
    reinforce remote rate limiting and are intentionally avoided.
    """
    delays = (0, 25)
    last_error: Exception | None = None

    for attempt, delay in enumerate(delays, start=1):
        if delay:
            print(f"SSE HTTP 403 retry: waiting {delay}s before attempt {attempt}")
            time.sleep(delay)
        try:
            payload = _curl_once(trade_date)
            break
        except SSEForbiddenError as exc:
            last_error = exc
            if attempt == len(delays):
                raise
    else:
        raise SSEError(str(last_error))

    records = payload.get("result")
    if not isinstance(records, list) or not records:
        raise SSEError("SSE JSON does not contain a non-empty result list")

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    normalized: list[dict] = []
    dates: set[str] = set()
    for row in records:
        actual_date = str(row.get("STAT_DATE", "")).strip()
        code = str(row.get("SEC_CODE", "")).strip().zfill(6)
        name = str(row.get("SEC_NAME", "")).strip()
        raw_share = str(row.get("TOT_VOL", "")).replace(",", "").strip()
        if not actual_date or not code or not name or not raw_share:
            raise SSEError(f"SSE row has missing required fields: {row}")
        try:
            total_share = Decimal(raw_share)
        except InvalidOperation as exc:
            raise SSEError(f"Invalid SSE total share: {raw_share}") from exc
        if total_share < 0:
            raise SSEError(f"Negative SSE total share: {row}")
        dates.add(actual_date)
        normalized.append(
            {
                "trade_date": actual_date,
                "exchange": "SSE",
                "fund_code": code,
                "fund_name": name,
                "etf_type": row.get("ETF_TYPE") or None,
                "total_share": str(total_share),
                "source": "SSE_API",
            }
        )

    if dates != {trade_date}:
        raise SSEError(f"SSE date mismatch: expected {trade_date}, received {sorted(dates)}")
    if len(normalized) < 700:
        raise SSEError(f"Unexpectedly low SSE record count: {len(normalized)}")
    if len({r['fund_code'] for r in normalized}) != len(normalized):
        raise SSEError("Duplicate SSE fund codes detected")

    return normalized
