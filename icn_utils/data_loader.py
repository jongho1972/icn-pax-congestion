"""인천공항 국제선 예상 승객수 데이터 로더.

저장 구조: Daily_Data/passgr_YYYYMMDD.pkl
한 일자 1개 pkl에 T1+T2 9개 시트 통합 dict가 들어있다.

dict 스키마 (요약):
    {
      "date": "YYYYMMDD",
      "fetched_at": "...",
      "T1": {"depart", "arrive", "transit", "depart_route", "arrive_route",
             "shuttle_depart", "shuttle_arrive"},
      "T2": {... 동일 구조}
    }

자세한 시트별 구조는 icn_utils/excel_parser.py 참조.
"""
from __future__ import annotations

import logging
import os
import pickle
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger("icn_pax_congestion.data_loader")

KST = ZoneInfo("Asia/Seoul")


def _load_pkl(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            data = pickle.load(f)
    except Exception as exc:
        logger.warning("pkl load failed %s: %r", path, exc)
        return None
    if not isinstance(data, dict) or "T1" not in data or "T2" not in data:
        logger.warning("pkl schema invalid (missing T1/T2): %s", path)
        return None
    return data


def load_day(daily_dir: str, ymd: str) -> tuple[Optional[dict], str]:
    """주어진 날짜의 통합 dict를 반환.

    Returns: (data, source) — source ∈ {"excel", "none"}
    """
    p = os.path.join(daily_dir, f"passgr_{ymd}.pkl")
    data = _load_pkl(p)
    if data is None:
        return None, "none"
    return data, "excel"


def load_range(
    daily_dir: str, start: date, end: date
) -> dict[str, tuple[Optional[dict], str]]:
    """start ~ end (양끝 포함) 범위의 일별 통합 dict를 dict로 반환."""
    out: dict[str, tuple[Optional[dict], str]] = {}
    cur = start
    while cur <= end:
        ymd = cur.strftime("%Y%m%d")
        out[ymd] = load_day(daily_dir, ymd)
        cur += timedelta(days=1)
    return out


def list_available_dates(daily_dir: str) -> list[str]:
    """Daily_Data 에 데이터가 있는 날짜(YYYYMMDD) 정렬 리스트.

    `passgr_YYYYMMDD.pkl` 패턴만 인식하며 _archive_openapi/ 같은
    하위 디렉토리·구버전 _d0/_d1/_web 접미 파일은 무시한다.
    """
    if not os.path.isdir(daily_dir):
        return []
    seen: set[str] = set()
    for name in os.listdir(daily_dir):
        if not name.startswith("passgr_") or not name.endswith(".pkl"):
            continue
        # passgr_YYYYMMDD.pkl (suffix 없음만 채택)
        stem = name[len("passgr_"):-len(".pkl")]
        if len(stem) != 8 or not stem.isdigit():
            continue
        try:
            datetime.strptime(stem, "%Y%m%d")
            seen.add(stem)
        except ValueError:
            continue
    return sorted(seen)
