from __future__ import annotations

import math
import os
from datetime import date

import requests


class Serv00Client:
    def __init__(self, base_url: str | None = None, token: str | None = None, timeout: int = 60):
        self.base_url = (base_url or os.environ.get("ETF_API_URL", "")).rstrip("/")
        self.token = token or os.environ.get("ETF_API_TOKEN", "")
        self.timeout = timeout
        if not self.base_url or not self.token:
            raise RuntimeError("ETF_API_URL and ETF_API_TOKEN are required")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def health(self) -> dict:
        response = self.session.get(f"{self.base_url}/api/v1/health", timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def import_exchange(
        self,
        exchange: str,
        trade_date: date,
        records: list[dict],
        batch_size: int = 300,
        github_run_id: str | None = None,
        github_commit: str | None = None,
    ) -> list[dict]:
        batch_count = math.ceil(len(records) / batch_size)
        responses: list[dict] = []
        for index in range(batch_count):
            start = index * batch_size
            batch = records[start : start + batch_size]
            payload = {
                "exchange": exchange,
                "trade_date": trade_date.isoformat(),
                "records": batch,
                "batch_no": index + 1,
                "batch_count": batch_count,
                "finalize": index + 1 == batch_count,
                "github_run_id": github_run_id,
                "github_commit": github_commit,
            }
            response = self.session.post(
                f"{self.base_url}/api/v1/import",
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
            if data.get("success") is not True or data.get("committed") is not True:
                raise RuntimeError(f"Serv00 import failed: {data}")
            responses.append(data)
        return responses
