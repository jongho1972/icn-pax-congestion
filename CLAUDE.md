# ICN Pax Congestion — 인천공항 국제선 예상 승객수

airport.kr 공식 통계 페이지의 '엑셀 다운로드' 엔드포인트로 D-0(오늘) ~ D+1(내일) 출국·입국·환승·노선·셔틀트레인 정보를 매일 받아 시각화하는 대시보드 (FastAPI + Plotly.js → Render).

## 구성

| 파일 | 역할 |
|------|------|
| `main.py` | FastAPI 앱 — `/`, `/api/refresh` (POST, X-Refresh-Token), `/healthz`, `/api/export-raw`. 메모리 48시간 캐시 + dogpile 락. `ICN_TODAY_OVERRIDE=YYYYMMDD` 환경변수로 임의 일자 시뮬 가능 (디버그용) |
| `templates/index.html` | Jinja2 템플릿 — 비번 게이트 + **핵심 요약 카드(SMS 동일 기준 + 환율 + ▲▼ MTD 대비)** + 출국장 평면도 SVG 2개 + 일자별 표 + 시간대별 라인 2개 + 도착지별(권역) 그룹바 2개 |
| `icn_utils/excel_parser.py` | airport.kr 엑셀 9개 시트 파서 (시트별 함수 + `parse_terminal` 통합) |
| `icn_utils/exchange_rate.py` | dutyfreemania.com에서 면세점 고시환율(USD/KRW) 스크래핑 (서울외국환중개 전일 고시 = 모든 면세점 공통) |
| `icn_utils/data_loader.py` | `load_day(daily_dir, ymd)` → 통합 dict 반환. 단일 pkl 로드 |
| `icn_utils/aggregator.py` | KPI·MTD·시간대·gate별·노선별·예약합계(SMS 기준) 집계 + delta 계산 |
| `backfill_excel.py` | airport.kr xls 다운(인증 없음, GET 1회) → 9개 시트 파싱 → 통합 dict pkl 저장 + 환율 동시 갱신 |
| `Daily_Data/` | 일자별 통합 pkl `passgr_YYYYMMDD.pkl` + `exchange_rates.pkl` (환율 30+일치 누적) |
| `Daily_Data/_archive_openapi/` | 구 OpenAPI 시절 pkl 보관 (사용 중지) |
| `render.yaml` | Render 배포 설정. **buildFilter 사용 금지** (컨테이너 데이터 동기화 부작용으로 2026-05-09 제거) |
| `requirements.txt` | fastapi, uvicorn, jinja2, pandas, requests, **xlrd**, holidays |
| `.env` | `REFRESH_TOKEN` (gitignore) |

## 접근 제어
- 비번 `0708`, sessionStorage 키 `pax_congestion_auth_ok` (다른 신라 사이트와 분리)
- 게이트는 `visibility:hidden` 방식 (Plotly 차트 너비 정상 측정 보장)
- 세션 복원 감지(`navType==='back_forward' && !sameOriginRef`) 시 인증 무효화

## 데이터 수급 흐름

- **다운로드 URL**: `https://www.airport.kr/pni/ap_ko/statisticPredictCrowdedOfInoutExcel.do?selTm={T1|T2}&pday=YYYYMMDD`
  - 인증·세션 불필요. GET 1회로 ~329KB .xls 응답 (Composite Document V2)
  - 매일 **17:00 KST** 갱신 (D+1까지 출입국·환승·노선·셔틀, D+2까지 출국 한정)
- **수집 정책 (매일 17:05 KST + 23:30 KST 2회)**:
  - `Daily_Data/passgr_YYYYMMDD.pkl` — 그날 D-0 + 다음날 D+1 (덮어쓰기 — 23:30 cron이 최종값)
- **캐싱**: 메모리 dict (TTL 48시간, dogpile 락). 단일 워커 가정.
- **캐시 갱신**: 17:10 KST + 23:35 KST에 GitHub Actions cron이 `/api/refresh` 호출

## 엑셀 시트 9개

| 시트 | 추출 데이터 |
|---|---|
| 출국승객예고 | 예약합계(출국/입국) + 출국장별 합계 + 동/서 비율 + **시간대 × 출국장** 매트릭스 |
| 입국승객예고 | 시간대 × 입국심사대 매트릭스 (T1: AB/C/D/EF, T2: A/B) |
| 환승객예고 | 대한항공·아시아나 비율 + 보안검색대별 환승객 |
| 출국노선별승객예고 | **권역(7개) 합계 + 시간대 × 권역 매트릭스** ★ 마케팅 핵심 |
| 입국노선별승객예고 | 권역별 입국 (raw 보관, 화면 미사용) |
| 출국·입국 셔틀트레인 | T1↔탑승동 셔틀 시간대별 객수 (raw 보관) |
| basedata | 모든 데이터 raw 백업 (현재 미사용) |

**권역 7개**: 일본 / 중국 / 동남아 / 미주 / 유럽 / 오세아니아 / 기타

## 통합 dict 스키마 (`Daily_Data/passgr_YYYYMMDD.pkl`)

```python
{
  "date": "20260509",
  "fetched_at": "2026-05-09 23:30 KST",
  "T1": {
    "depart": {"예약합계": {...}, "출국장별": {"1","2","3","4","5_6"}, "동서비율": {...}, "시간대별": [24개 dict]},
    "arrive": {"심사대별": {"AB","C","D","EF"}, "시간대별": [24개]},
    "transit": {"KE", "OZ", "계", "비율_KE", "보안검색대별"},
    "depart_route": {"권역합계": {7권역}, "시간대별": [24개]},
    "arrive_route": {... 동일},
    "shuttle_depart": [24개 dict],
    "shuttle_arrive": [24개 dict]
  },
  "T2": {... 동일 (출국장 1·2 / 입국심사대 A·B)}
}
```

## 시각화

화면 전체가 **focus 일자**(`focus_is_tomorrow` flag)에 동기화됨:
- 내일 데이터 발표됨(`reserved.tomorrow.total > 0`) → 카드/평면도/시간대별/도착지별 모두 **내일** 데이터
- 미발표 → 모두 **오늘** 데이터로 fallback
- 차트 trace 이름도 동적("오늘 예상" / "내일 예상")

1. **핵심 요약 카드** (SMS 알림과 동일 기준 — 항공사 예약 출국, 환승객 포함):
   - 전체/T1/T2 출국객수 + ▲▼ MTD 대비 변화율 + 환율 칩($1=₩XXX)
   - 톤: 화이트 + 좌측 크림슨 strip + 골드 환율 칩
2. **출국장 평면도 SVG 2개** (T1: 5번/2번/3번/4번/5·6번 5개 zone, T2: 1번/2번 2개 zone). 5·6번은 원본 엑셀 합산값 그대로 단일 박스
3. **일자별 표**: 날짜·요일·T1·T2·피크 시간/객수, 오늘=파랑/미래=노랑/주말=빨강
4. **시간대별 차트** (T1·T2 분리): MTD 평균(점선) + focus 일자(실선). y축 동일
5. **도착지별 7권역 그룹바 2개** (T1·T2): MTD 평균 vs focus 일자, y축 통일

각주: 데이터 출처 → 산출 방식 → 통계 기준(카드 vs 그 외) → 5·6번 합산 → 환율 출처 순.

## 로컬 실행

```bash
cd 출국장이용객수조회
pip install -r requirements.txt
python3 backfill_excel.py 20260509  # 임의 일자 수집
uvicorn main:app --reload --port 8000
```

## 배포

- **Repo**: `jongho1972/icn-pax-congestion` (별도 git 저장소, private)
- **Render URL**: <https://jhawk-pax-congestion.onrender.com>
- **Env (Render Dashboard)**: `REFRESH_TOKEN` (1개)

## 자동화

- **GitHub Actions** `.github/workflows/daily-backfill.yml`
  - 스케줄: **17:05 KST + 23:30 KST** (하루 2회)
  - 동작: `actions/checkout` → `pip install pandas requests xlrd` → `python3 backfill_excel.py` → `git add Daily_Data/` → 변경 있으면 commit `data: backfill YYYYMMDD-HHMMKST` 후 push. 23:30 KST run에서만 Render Deploy Hook 호출.
- **GitHub Actions** `.github/workflows/refresh-cache.yml`
  - 스케줄: **17:10 KST + 23:35 KST** (backfill 5분 후)
  - `POST /api/refresh` (헤더 `X-Refresh-Token`)
- **GitHub Actions** `.github/workflows/keep-alive.yml` — 10분마다 GET / (콜드 슬립 방지)
- **GitHub Actions** `.github/workflows/daily-mailer.yml`
  - 스케줄: **17:30 KST**
  - Playwright headless chromium → 비번 입력 → `body.capturing` + 1.5배 zoom → `.container` PNG 캡처 → SMTP 발송
  - 수신자: `mailing_list.txt` 우선, 없으면 `MAIL_RECIPIENTS` 환경변수 폴백
  - 실패 시 `jongho1972@gmail.com` 자동 통지

## 신라 사이트 연동

- 신라면세점 루트 랜딩(`shilla-icn-mkt.netlify.app`) 5번째 카드: "인천공항 국제선 예상 승객수" / 외부 Render URL 새 탭

## 참고

- airport.kr 통계 페이지: <https://www.airport.kr/ap_ko/883/subview.do>
- 엑셀에 D+2 출국 데이터까지 포함되지만 본 PoC는 D+1까지만 사용
- D+1 데이터가 17:00 KST 발표 전에는 비어있을 수 있음 — 화면에서 0으로 표시
