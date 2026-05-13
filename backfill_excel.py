"""airport.kr 공항 예상 혼잡도 엑셀 다운로더 + 통합 pkl 저장.

매일 17:05 KST + 23:30 KST cron에서 호출. 인천공항 통계 페이지의
'엑셀 다운로드' 엔드포인트(인증 없음, GET 1회)로 T1·T2 두 파일을 받아
9개 시트를 모두 파싱한 후 단일 dict로 저장한다.

저장 파일: Daily_Data/passgr_YYYYMMDD.pkl  (T1+T2 통합)
실행:      python3 backfill_excel.py [YYYYMMDD ...]  (인자 없으면 오늘+내일)
"""
from __future__ import annotations

import os
import pickle
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from icn_utils.excel_parser import parse_terminal
from icn_utils.exchange_rate import fetch_rates, save_rates

KST = ZoneInfo("Asia/Seoul")
DOWNLOAD_URL = "https://www.airport.kr/pni/ap_ko/statisticPredictCrowdedOfInoutExcel.do"
DAILY_DIR = Path(__file__).resolve().parent / "Daily_Data"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


_SESSION = _make_session()


def fetch_excel(ymd: str, terminal: str) -> bytes:
    """단일 (T1|T2, 날짜) 엑셀 바이트 다운로드. 비-엑셀 응답이면 ValueError."""
    r = _SESSION.get(
        DOWNLOAD_URL,
        params={"selTm": terminal, "pday": ymd},
        headers={"User-Agent": UA, "Accept": "*/*"},
        timeout=60,
    )
    r.raise_for_status()
    ct = r.headers.get("content-type", "")
    if "msdownload" not in ct and not r.content.startswith(b"\xd0\xcf\x11\xe0"):
        # CDF v2 매직 시그니처 확인 — HTML 응답이면 인증 페이지 등 비정상
        raise ValueError(f"non-excel response: ct={ct!r} size={len(r.content)}")
    return r.content


def collect_day(ymd: str) -> dict:
    """한 날짜의 T1+T2 엑셀을 받아 통합 dict 반환."""
    t1_bytes = fetch_excel(ymd, "T1")
    t2_bytes = fetch_excel(ymd, "T2")
    return {
        "fetched_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
        "date": ymd,
        "T1": parse_terminal(t1_bytes, "T1"),
        "T2": parse_terminal(t2_bytes, "T2"),
    }


def save_day(ymd: str, data: dict) -> Path:
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    path = DAILY_DIR / f"passgr_{ymd}.pkl"
    with open(path, "wb") as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    # 인트라데이 검증용 추가 스냅샷 (한시적, ~2026-05-20)
    # 같은 target date를 다른 fetch 시각에 받아 airport.kr이 17:00 외에도 갱신하는지 검증
    # 디렉토리: Daily_Data/_verification/<targetYMD>/<fetchYMDHHMM>.pkl
    if os.environ.get("VERIFY_INTRADAY", "1") != "0":
        verify_dir = DAILY_DIR / "_verification" / ymd
        verify_dir.mkdir(parents=True, exist_ok=True)
        fetch_stamp = datetime.now(KST).strftime("%Y%m%d_%H%M")
        verify_path = verify_dir / f"{fetch_stamp}.pkl"
        with open(verify_path, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    return path


def _summary(data: dict) -> str:
    """저장 직후 한 줄 요약 (검증용)."""
    t1 = sum(data["T1"]["depart"]["출국장별"].values())
    t2 = sum(data["T2"]["depart"]["출국장별"].values())
    routes_t1 = sum(data["T1"]["depart_route"]["권역합계"].values())
    return f"T1출국={t1:,} T2출국={t2:,} T1권역합={routes_t1:,}"


def main(argv: list[str]) -> int:
    today = datetime.now(KST).date()
    if len(argv) > 1:
        targets = argv[1:]  # 명시 인자 사용
    else:
        targets = [
            today.strftime("%Y%m%d"),
            (today + timedelta(days=1)).strftime("%Y%m%d"),
        ]

    saved = 0
    failures = []
    for ymd in targets:
        try:
            data = collect_day(ymd)
            path = save_day(ymd, data)
            saved += 1
            print(f"  {ymd}: {_summary(data)} → {path.name}")
        except Exception as exc:
            failures.append((ymd, repr(exc)))
            print(f"  {ymd}: FAILED — {exc!r}", file=sys.stderr)

    # 환율 수집 (실패해도 메인 backfill 결과에는 영향 없음)
    try:
        rates = fetch_rates()
        if rates:
            save_rates(DAILY_DIR, rates)
            print(f"  환율: {len(rates)}일치 갱신 (오늘 {rates.get(today.strftime('%Y%m%d'), '—')})")
        else:
            print("  환율: 수집 실패 (빈 응답)", file=sys.stderr)
    except Exception as exc:
        print(f"  환율: FAILED — {exc!r}", file=sys.stderr)

    print(f"완료 ({saved}/{len(targets)})")
    return 0 if not failures else 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
