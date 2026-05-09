"""면세점 고시환율 (USD/KRW) 수집기.

면세점들은 모두 **서울외국환중개(주)의 전일 고시환율**을 공통 적용한다.
즉 오늘 서울외국환중개의 환율이 내일의 면세점 환율이 된다.

dutyfreemania.com이 이를 매일 정리해 ~30일치 + 내일(D+1) 환율까지 한 페이지에
google.charts.DataTable JS 형태로 노출한다. 동일 출처라 SMS 알림 환율과 100% 일치.

저장 형식: Daily_Data/exchange_rates.pkl  (dict[YYYYMMDD] = float, 덮어쓰기)
"""
from __future__ import annotations

import logging
import pickle
import re
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("icn_pax_congestion.exchange_rate")

SOURCE_URL = "http://dutyfreemania.com/"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

# google.charts 데이터: `[new Date(YYYY,M0,DD), 1450.8, 1450.8]`
# M0는 JS Date 월(0-indexed) — 5월=4
_ROW_RE = re.compile(
    r"new\s+Date\(\s*(\d{4})\s*,\s*(\d{1,2})\s*,\s*(\d{1,2})\s*\)\s*,\s*([\d.]+)"
)


def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3, backoff_factor=1.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET",), raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


_SESSION = _make_session()


def fetch_rates() -> dict[str, float]:
    """dutyfreemania.com에서 USD/KRW 일별 환율 dict를 받아 반환.

    Returns: {"YYYYMMDD": rate, ...}  — 보통 ~30일치 + D+1 한 일자 포함.
    HTML 구조 변경 등으로 파싱 실패 시 빈 dict 반환.
    """
    try:
        r = _SESSION.get(SOURCE_URL, headers={"User-Agent": UA}, timeout=30, verify=False)
        r.raise_for_status()
    except Exception as exc:
        logger.warning("fetch_rates failed: %r", exc)
        return {}
    rates: dict[str, float] = {}
    for m in _ROW_RE.finditer(r.text):
        year = int(m.group(1))
        month0 = int(m.group(2))
        day = int(m.group(3))
        rate = float(m.group(4))
        # JS Date 월은 0-indexed → +1
        ymd = f"{year:04d}{month0 + 1:02d}{day:02d}"
        rates[ymd] = rate
    return rates


def save_rates(daily_dir: Path, rates: dict[str, float]) -> Path:
    """기존 환율 dict와 병합 후 저장 (새 데이터가 우선)."""
    daily_dir.mkdir(parents=True, exist_ok=True)
    path = daily_dir / "exchange_rates.pkl"
    existing: dict[str, float] = {}
    if path.exists():
        try:
            with open(path, "rb") as f:
                existing = pickle.load(f) or {}
        except Exception:
            existing = {}
    merged = {**existing, **rates}
    with open(path, "wb") as f:
        pickle.dump(merged, f, protocol=pickle.HIGHEST_PROTOCOL)
    return path


def load_rates(daily_dir: Path) -> dict[str, float]:
    """저장된 환율 dict 로드 (없으면 빈 dict)."""
    path = daily_dir / "exchange_rates.pkl"
    if not path.exists():
        return {}
    try:
        with open(path, "rb") as f:
            data = pickle.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.warning("load_rates failed: %r", exc)
        return {}
