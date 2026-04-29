"""인천공항 출국장 혼잡도 일별 수집 스크립트 (cron 자동 실행용).

API: getPassgrAnncmt — D-0(오늘) / D+1(내일) 시간대별 예상 이용객수.
한 번의 호출로 입국장(eg) + 출국장(dg) 모두 반환되므로 둘 다 저장한다.

환경변수 INCHEON_API_KEY 필요.
실행: python3 backfill.py
결과: Daily_Data/passgr_YYYYMMDD_d{0,1}.pkl
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests

KST = ZoneInfo("Asia/Seoul")
API_URL = "http://apis.data.go.kr/B551177/passgrAnncmt/getPassgrAnncmt"

NUMERIC_COLS = [
    "t1eg1", "t1eg2", "t1eg3", "t1eg4", "t1egsum1",
    "t1dg1", "t1dg2", "t1dg3", "t1dg4", "t1dg5", "t1dg6", "t1dgsum1",
    "t2eg1", "t2eg2", "t2egsum1",
    "t2dg1", "t2dg2", "t2dgsum2",
]


def fetch(service_key: str, selectdate: int) -> pd.DataFrame:
    """selectdate: 0=오늘, 1=내일. items가 비거나 누락되면 빈 DF 반환."""
    params = {
        "serviceKey": service_key,
        "type": "json",
        "selectdate": selectdate,
        "numOfRows": 100,
    }
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    try:
        body = r.json()["response"]["body"]
    except (KeyError, ValueError):
        return pd.DataFrame()
    items = body.get("items") or []
    if not items:
        return pd.DataFrame()
    df = pd.DataFrame(items)
    if "adate" not in df.columns or "atime" not in df.columns:
        return pd.DataFrame()
    # 숫자 컬럼 안전 변환 (누락 컬럼은 0 채움)
    for c in NUMERIC_COLS:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df


def main() -> int:
    service_key = os.environ.get("INCHEON_API_KEY")
    if not service_key:
        sys.stderr.write("환경변수 INCHEON_API_KEY 가 필요합니다.\n")
        return 1

    base = os.path.dirname(os.path.abspath(__file__))
    daily_dir = os.path.join(base, "Daily_Data")
    os.makedirs(daily_dir, exist_ok=True)

    today = datetime.now(KST).date()
    tomorrow = today + timedelta(days=1)
    targets = [(0, today), (1, tomorrow)]

    saved = 0
    for sel, dt in targets:
        label = "D-0(오늘)" if sel == 0 else "D+1(내일)"
        ymd = dt.strftime("%Y%m%d")
        try:
            df = fetch(service_key, sel)
        except Exception as exc:
            print(f"  {label} {ymd} 오류: {exc!r}")
            continue
        if df.empty:
            print(f"  {label} {ymd}: 응답 비어있음 (skip)")
            continue
        # API의 adate 가 요청한 날짜와 다르면 응답 기준 사용
        api_dates = df["adate"].dropna().unique().tolist()
        api_ymd = api_dates[0] if api_dates else ymd
        suffix = "d0" if sel == 0 else "d1"
        out = os.path.join(daily_dir, f"passgr_{api_ymd}_{suffix}.pkl")
        df.to_pickle(out)
        saved += 1
        print(f"  {label} {api_ymd}: {len(df)}건 저장 → {os.path.basename(out)}")

    print(f"완료 ({saved}/{len(targets)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
