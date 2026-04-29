# ICN Pax Congestion — 인천공항 출국장 혼잡도

인천국제공항공사 OpenAPI(`getPassgrAnncmt`)로 D-0(오늘) ~ D+1(내일) 시간대별 출국장 예상 이용객수를 조회·시각화하는 대시보드 (FastAPI + Plotly.js → Render).

## 구성

| 파일 | 역할 |
|------|------|
| `main.py` | FastAPI 앱 — `/`, `/api/refresh` (POST, X-Refresh-Token), `/healthz`, `/api/export-raw`. 메모리+/tmp 디스크 48시간 캐시 |
| `templates/index.html` | Jinja2 템플릿 — 비번 게이트 + Plotly 차트 4종(시간대별 T1·T2, 출국장별 stacked, 일자별 추이) + 일자별 표 |
| `icn_utils/data_loader.py` | API 호출 + Daily_Data/ pkl 로드 + D+1 빈 응답 강건 처리 |
| `icn_utils/aggregator.py` | 시간대별·일자별·출국장별 집계 + KPI 계산 |
| `backfill.py` | cron용 일별 수집 스크립트 — D-0/D+1 둘 다 호출 |
| `Daily_Data/` | 일별 원본 pkl (`passgr_YYYYMMDD_d{0,1}.pkl`), 매일 누적 |
| `render.yaml` | Render 배포 설정 (python runtime, uvicorn) |
| `requirements.txt` | fastapi, uvicorn, jinja2, pandas, requests 등 |
| `.env` | `INCHEON_API_KEY`, `REFRESH_TOKEN` (gitignore) |
| `인천공항API_출국장혼잡도.ipynb` | 원본 노트북 (참고용) |

## 접근 제어
- 비번 `0708`, sessionStorage 키 `pax_congestion_auth_ok` (다른 신라 사이트와 분리)
- 게이트는 `visibility:hidden` 방식 (Plotly 차트 너비 정상 측정 보장)
- 세션 복원 감지(`navType==='back_forward' && !sameOriginRef`) 시 인증 무효화

## 데이터 수급 흐름

- **API 한계**: D-0(오늘 부분실측+예측) / D+1(내일 예측) 만 반환. 과거 데이터 미제공 → **반드시 매일 누적 수집 필요**
- **수집 정책 (매일 23:30 KST)**:
  - `Daily_Data/passgr_YYYYMMDD_d0.pkl` — 그날 D-0 호출분 (실측 근사 = 최종값으로 채택)
  - `Daily_Data/passgr_YYYYMMDD_d1.pkl` — 다음날 D+1 예측분 (백업·검증용)
- **조회 우선순위**: 같은 날짜에 d0가 있으면 d0, 없으면 d1, 둘 다 없고 오늘·내일이면 라이브 API fallback
- **캐싱**: 메모리 + 디스크 pickle 이중 캐시 (`/tmp/icn_pax_congestion_cache.pkl`). TTL 48시간(cron 누락 안전 마진). 키 = 오늘 날짜
- **캐시 갱신**: 매일 23:35 KST에 GitHub Actions cron이 `/api/refresh` 호출

## API 응답 구조

한 번의 호출로 **입국장(eg) + 출국장(dg)** 시간대별 객수가 모두 반환됨. 25행(시간대 24 + 합계 1).

| 컬럼 | 의미 |
|---|---|
| `adate` / `atime` | 날짜(YYYYMMDD) / 시간대(`00_01` ~ `23_24`) |
| `t1eg1~4` / `t1egsum1` | T1 입국장 1~4 / 합계 |
| `t1dg1~6` / `t1dgsum1` | T1 출국장 D1~D6 / 합계 |
| `t2eg1~2` / `t2egsum1` | T2 입국장 / 합계 |
| `t2dg1~2` / `t2dgsum2` | T2 출국장 D1·D2 / 합계 |

화면에서는 출국장(dg)만 사용. 입국장(eg) 데이터는 raw에 함께 저장만 하고 향후 확장 여지로 보존.

## 시각화

1. **KPI 카드 2개**: 오늘 / 내일 — 총객수, T1·T2 분리, 피크 시간대·객수
2. **시간대별 차트** (T1·T2 분리 2 패널)
   - 실선 = 오늘 (D-0) / 점선 = 내일 (D+1)
3. **출국장별 시간대 분포** (오늘 기준 stacked bar)
   - T1 D1~D6 (파랑 계열) + T2 D1·D2 (주황 계열)
4. **일자별 추이** (D-29 ~ D+1, 30일)
   - T1·T2 + 합계(점선) · 미래일 노란 배경
5. **일자별 표**
   - 날짜·요일·T1·T2·합계·피크 시간대·피크 객수·출처(실측/예측/실시간/없음)
   - 오늘 = 파랑 하이라이트 / 미래 = 노란 배경 / 주말 = 빨강

## 로컬 실행

```bash
cd 출국장이용객수조회
pip install -r requirements.txt
INCHEON_API_KEY="..." uvicorn main:app --reload --port 8000
```

`.env`에 `INCHEON_API_KEY` 필요. Daily_Data가 비어있으면 라이브 API로 fallback 가능.

## 배포

- **Repo**: `jongho1972/icn-pax-congestion` (별도 git 저장소, private)
- **Render URL**: <https://jhawk-pax-congestion.onrender.com>
- **Env (Render Dashboard)**: `INCHEON_API_KEY`, `REFRESH_TOKEN`

## 자동화

- **Claude Code 스케줄 트리거** (Daily_Data 수집)
  - 스케줄: 매일 23:30 KST (= 14:30 UTC)
  - 동작: 원격 에이전트가 레포 clone → `backfill.py` 실행 → `Daily_Data/` 갱신 → 변경 있으면 `git push origin main`
  - 출발항공편(17:00 KST)과 시간 분리해서 트리거 충돌 방지
- **GitHub Actions** `.github/workflows/keep-alive.yml`
  - 스케줄: 10분마다 `/healthz` 핑 (Render 무료 슬립 방지)
- **GitHub Actions** `.github/workflows/refresh-cache.yml`
  - 스케줄: 매일 23:35 KST (= 14:35 UTC)
  - 동작: `POST /api/refresh` (헤더 `X-Refresh-Token: ${{ secrets.REFRESH_TOKEN }}`)
  - GitHub Secret 필요: `REFRESH_TOKEN`

## 신라 사이트 연동

- 신라면세점 루트 랜딩(`shilla-icn-mkt.netlify.app`) **5번째 카드**로 노출
- 카드 제목: "출국장 혼잡도 자료" / 외부 Render URL 새 탭

## 참고

- 인천공항 OpenAPI: <https://www.data.go.kr/data/15095066/openapi.do>
- D-0/D+1만 반환. 과거 데이터는 누적 pkl로만 보관
- D+1 예측은 인천공항 측 데이터 생성 시점에 따라 비어있을 수 있음 — `data_loader._fetch_api`에서 빈 응답 시 빈 DF 반환, `build_payload`에서 KPI 0 처리
