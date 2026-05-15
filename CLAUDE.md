# ICN Pax Congestion — 인천공항 국제선 출국객수

airport.kr 공식 통계 페이지의 '엑셀 다운로드' 엔드포인트로 D-0(오늘) ~ D+1(내일) 출국·입국·환승·노선·셔틀트레인 정보를 매일 받아 시각화하는 대시보드 (FastAPI + Plotly.js → Render).

## 구성

| 파일 | 역할 |
|------|------|
| `main.py` | FastAPI 앱 — `/`, `/api/refresh` (POST, X-Refresh-Token), `/healthz`, `/api/export-raw`. 메모리 48시간 캐시 + dogpile 락. `ICN_TODAY_OVERRIDE=YYYYMMDD` 환경변수로 임의 일자 시뮬 가능 (디버그용) |
| `templates/index.html` | Jinja2 템플릿 — 비번 게이트 + **핵심 요약 카드(SMS 동일 기준 + 환율 + ▲▼ MTD 대비)** + 일자별 섹션(T1·T2 추이 라인 2개 + D-29~D+1 표) + 월누적 섹션(요약+표) + 출국장 평면도 SVG 2개 + 도착지별(권역) 그룹바 2개 + 시간대별 라인 2개(맨 아래, MTD 동일일수 비교) |
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
- **수집 정책 (매일 17:25 KST + 23:30 KST 2회)**:
  - `Daily_Data/passgr_YYYYMMDD.pkl` — 그날 D-0 + 다음날 D+1 (덮어쓰기 — 23:30 cron이 최종값)
  - 17:25 시각 선택 이유: airport.kr 17:00 1차 갱신 후 D+1 항공편이 점진 추가됨. 2026-05-14 D+1 데이터가 17:05엔 부분 적재(`10798B`)로 17:30 메일이 불완전 발송됨 → 17:25로 20분 늦춰 D+1 완성도 확보
- **캐싱**: 메모리 dict (TTL 48시간, dogpile 락). 단일 워커 가정.
- **캐시 갱신**: 17:30 KST + 23:35 KST에 GitHub Actions cron이 `/api/refresh` 호출

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

화면 구조: **일자별 → 월누적 → 출국장별 → 도착지별 → 시간대별** (예약 베이스 두 섹션을 위에, 출국장 통과 베이스 세 섹션을 아래에 배치 — 출국장별 섹션 위에 베이스 안내 각주 1회).
- **핵심 요약 카드 / 일자별 추이 차트 / 일자별 표 / 월누적**은 모두 **예약합계 출국 기준**(환승객 포함, SMS 알림 동일). `daily_totals`·`daily_series_by_month`·`monthly_compare`는 `basis="reserved"` 인자로 호출, MTD 기준선은 `mtd_reserved`, 일자별 표 ▲▼ 비교는 `prev_dow_reserved_avg`.
- **출국장별 평면도 / 도착지별 권역 / 시간대별 차트**는 엑셀 `2.출국장별 예상여객`/`출국노선별`/`3.시간대별 출국장별` 시트가 raw 데이터로만 제공돼 **출국장 통과 기준**(환승객 제외) 유지. `gate_compare`·`route_compare`·`hourly_mtd_avg`·`hourly_per_gate`가 해당.
- 누적 계열(월누적·출국장별·도착지별·시간대별)은 모두 이번달 1~cutoff vs 전월 1~cutoff 동일일수 비교.

1. **핵심 요약 카드** (SMS 알림과 동일 기준 — 항공사 예약 출국, 환승객 포함):
   - 전체/T1/T2 출국객수 + ▲▼ MTD 대비 변화율 + 환율 칩($1=₩XXX)
   - 톤: 화이트 + 좌측 크림슨 strip + 골드 환율 칩
2. **일자별** 섹션:
   - T1/T2 일자별 추이 라인 2개 (이번달 실선+마커, 전월 점선, 전월 평균 가로선, D+1 수직선, 주말 빨강) — 항공편수와 동일 형태
   - 일자별 표 (D-29~D+1, 날짜·요일·T1·T2·피크 시간/객수, 오늘=파랑/미래=노랑/주말=빨강)
3. **월누적** 섹션 (이번달 vs 전월 동일일수):
   - 요약 텍스트 `T1+T2 기준 N명 (전월비 ±N%) · 일평균 N명`
   - 표 T1·T2 × {월누적·일평균} × {전월·이번달·전월비}
4. **출국장별 평면도 SVG 2개** (T1: 1번/2번/3번/4번/5·6번 5개 zone, T2: 1번/2번 2개 zone) — 5·6번 합산
   - 데이터: 이번달 1~cutoff 누적 + 전월 동일일수 누적 + ▲▼ %
   - 시간대 셀렉터(전체/오전/오후/저녁/심야)로 누적값을 시간대 그룹별로 슬라이스 가능 (서버가 24×zone 매트릭스 합계 노출)
5. **도착지별 7권역 그룹바 2개** (T1·T2): 전월 동일일수 누적 vs 이번달 누적, y축 통일
6. **시간대별 차트 2개** (T1·T2 분리, y축 통일): 이번달 1~cutoff MTD 시간대별 평균(실선) vs 전월 동일일수 MTD 평균(점선). `hourly_mtd_avg(daily_map, year, month, cutoff_day)` 산출.

각주: 데이터 출처 → 산출 방식 → 환율 출처 순.

## 로컬 실행

```bash
cd 출국장이용객수조회
pip install -r requirements.txt
python3 backfill_excel.py 20260509  # 임의 일자 수집
uvicorn main:app --reload --port 8000
```

## 배포

- **Repo**: `jongho1972/icn-pax-congestion` (별도 git 저장소, private)
- **URL**: <https://pax.j-hawk.kr>
- **Env (Render Dashboard)**: `REFRESH_TOKEN` (1개)

## 자동화

- **GitHub Actions** `.github/workflows/daily-backfill.yml`
  - 트리거: **cron-job.org 외부 트리거** (workflow_dispatch) — 17:25 KST + 23:30 KST 하루 2회
  - GH Actions schedule는 큐 지연(+1~3h)으로 메일러 발사 시각을 못 맞춰 2026-05-12 stale 데이터 발송 사고 발생 → cron-job.org 외부 호출로 이전 (메일러와 동일 패턴, [[project_daily_mailer_external_trigger]])
  - 17:25 선택 이유: 17:00 airport.kr 갱신 후 D+1 항공편이 점진 추가되며, 5/14 17:05 시점엔 부분 적재 상태였음. 23:30 이전에 한 번 더 받기 위한 절충 시각.
  - 동작: `actions/checkout` → `pip install pandas requests xlrd` → `python3 backfill_excel.py` → `git add Daily_Data/` → 변경 있으면 commit `data: backfill YYYYMMDD-HHMMKST` 후 push. push 발생 시 Render Deploy Hook 호출 (두 트리거 모두).
- **GitHub Actions** `.github/workflows/refresh-cache.yml`
  - 스케줄: **17:30 KST + 23:35 KST** (backfill 5분 후)
  - `POST /api/refresh` (헤더 `X-Refresh-Token`)
- **외부 cron** cron-job.org — 14분 간격 `GET /healthz`. Render 무료 슬립 방지 + 페이로드 캐시 워밍 (출발항공편조회와 동일 패턴, GH Actions 한도 절약)
- **GitHub Actions** `.github/workflows/daily-mailer.yml`
  - 스케줄: **17:50 KST** (백필 17:25 + 캐시 refresh 17:30 후 D+1 적재 여유 확보)
  - Playwright headless chromium → 비번 입력 → `body.capturing` + 1.5배 zoom → `.container` PNG 캡처 → SMTP 발송
  - 수신자: `mailing_list.txt` 우선(커밋됨, 41명, 항공편수 메일러와 동기화), 없으면 `MAIL_RECIPIENTS` 환경변수 폴백. 두 출처 모두 동일 리스트 유지
  - `workflow_dispatch` 입력 `test_recipient` 지원 — 입력 시 `Override mailing list (test only)` 스텝이 `mailing_list.txt`를 덮어써 해당 1명에게만 발송
  - 실패 시 `jongho1972@gmail.com` 자동 통지

## 신라 사이트 연동

- 신라면세점 루트 랜딩(`shilla-icn-mkt.netlify.app`) Live Data 02번 카드: "인천공항 국제선 출국객수" / 외부 Render URL 새 탭

## 참고

- airport.kr 통계 페이지: <https://www.airport.kr/ap_ko/883/subview.do>
- 엑셀에 D+2 출국 데이터까지 포함되지만 본 PoC는 D+1까지만 사용
- D+1 데이터가 17:00 KST 발표 전에는 비어있을 수 있음 — 화면에서 0으로 표시
