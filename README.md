# ICN Pax Congestion — 인천공항 출국장 혼잡도 대시보드

[airport.kr 통계 페이지](https://www.airport.kr/ap_ko/883/subview.do)의 엑셀 다운로드 엔드포인트로 **D-0(오늘) ~ D+1(내일)** 출국·입국·환승·노선·셔틀트레인 객수를 매일 받아 시각화하는 사내 대시보드.

- 라이브: <https://pax.jhawk.kr> (비번 `0708`)
- 백엔드: FastAPI + Plotly.js (단일 워커, 메모리 48시간 캐시 + dogpile 락)
- 배포: j-hawk VPS (Hetzner CAX11 ARM · Docker Compose + Caddy)

## 주요 기능

- **핵심 요약 카드**: 전체/T1/T2 출국객수(예약합계, 환승 포함) + ▲▼ MTD 대비 변화율 + 면세점 고시환율 칩
- **일자별 추이**: D-29~D+1 표 + T1·T2 라인 차트 (이번달 실선, 전월 점선, 전월 평균선, D+1 수직선, 주말 빨강)
- **월누적 비교**: 전월 동일일수 vs 이번달
- **출국장 평면도 SVG**: T1 5개 zone(1·2·3·4·5·6) / T2 2개 zone(1·2), 시간대 셀렉터(전체/오전/오후/저녁/심야)
- **도착지별 7권역 그룹바**: 일본·중국·동남아·미주·유럽·오세아니아·기타
- **시간대별 차트**: 이번달 MTD 평균(실선) vs 전월 동일일수 MTD 평균(점선)

> 일자별·월누적·요약 카드는 **항공사 예약합계 출국 기준**(환승 포함, SMS 알림 동일). 출국장·도착지·시간대 차트는 엑셀 시트 특성상 **출국장 통과 기준**(환승 제외).

## 데이터 수급

- **다운로드 URL**: `https://www.airport.kr/pni/ap_ko/statisticPredictCrowdedOfInoutExcel.do?selTm={T1|T2}&pday=YYYYMMDD`
- 인증·세션 불필요. GET 1회로 ~329KB `.xls` 응답
- airport.kr 17:00 KST 1차 갱신 후 D+1이 점진 적재
- 수집 시각: **17:25 KST + 23:30 KST** 하루 2회 (23:30이 최종값)
- 엑셀 9개 시트 파싱: 출국승객예고 · 입국승객예고 · 환승객예고 · 출국노선별 · 입국노선별 · 출국·입국 셔틀트레인 · basedata

## 통합 dict 스키마

```python
# Daily_Data/passgr_YYYYMMDD.pkl
{
  "date": "20260509",
  "fetched_at": "2026-05-09 23:30 KST",
  "T1": {
    "depart": {"예약합계": {...}, "출국장별": {...}, "동서비율": {...}, "시간대별": [24]},
    "arrive": {"심사대별": {...}, "시간대별": [24]},
    "transit": {"KE", "OZ", "계", "비율_KE", "보안검색대별"},
    "depart_route": {"권역합계": {7권역}, "시간대별": [24]},
    "arrive_route": {...},
    "shuttle_depart": [24], "shuttle_arrive": [24]
  },
  "T2": {...}
}
```

## 자동화

| 워크플로우 | 시각 (KST) | 동작 |
|---|---|---|
| `daily-backfill.yml` | 17:25 · 23:30 | airport.kr xls → 9시트 파싱 → pkl 갱신 + 환율 갱신 → git push |
| `refresh-cache.yml` | 17:30 · 23:35 | `POST /api/refresh` (X-Refresh-Token) |
| `daily-mailer.yml` | 17:50 | Playwright 캡처(`body.capturing` + 1.5× zoom) → SMTP 발송 (수신자 41명) |
| cron-job.org | 14분 간격 | `GET /healthz` 캐시 워밍 |

> GH Actions schedule는 큐 지연(+1~3h)으로 메일러 시각을 못 맞춤 → 데이터 수집·캐시 갱신은 **cron-job.org 외부 트리거**(`workflow_dispatch`)로 이전.

## 로컬 실행

```bash
pip install -r requirements.txt
python3 backfill_excel.py 20260509   # 임의 일자 수집
uvicorn main:app --reload --port 8000
```

`.env`에 `REFRESH_TOKEN` 필요.

## 환경변수

| 키 | 용도 |
|---|---|
| `REFRESH_TOKEN` | `/api/refresh` 요청 인증 (`X-Refresh-Token` 헤더) |
| `ICN_TODAY_OVERRIDE=YYYYMMDD` | 임의 일자 시뮬레이션 (디버그) |

## 라이선스

내부용 (private). 데이터 출처: airport.kr 통계 페이지 · dutyfreemania.com(면세점 고시환율).
