# 📈 InvestBot

> **Kiwoom API 기반의 실시간 마켓 데이터 수집, 트레이딩 전략 수행 및 S3 Data Lakehouse 이관을 담당하는 Airflow 기반 데이터 파이프라인입니다.**
> 
> 금융 API 제약과 네트워크 불확실성 환경에서도 데이터 누락 없이 자가 복구가 가능하도록 **Dynamic Sliding Window**와 **UPSERT 기반의 멱등성 파이프라인**을 설계하고 구현했습니다.

---

## 📌 1. DAG 개발 및 파이프라인 현황

| 서비스 도메인 | 대표 DAG ID | 스케줄 주기 | 현재 상태 | 주요 역할 |
| :--- | :--- | :--- | :---: | :--- |
| **마켓 데이터 공급** | `dag_market` | `평일 08:00~15:55 KST (5분 슬롯) | **`완료`** | Kiwoom API 종목 마스터 갱신 및 5분봉 OHLCV 수집/이력 적재 |
| **전략 분석 및 매매** | `dag_trading` | Event-driven | `예정` | 계좌 정보 수집, AI 밴드 분석 및 매수/매도 시그널 주문 집행 |
| **DW 이관 (S3)** | `dag_dw_migration` | Daily (새벽 1회) | `예정` | Data Quality Gate (PySpark) 검증 후 S3 Bronze 적재 |

👉 자세한 DAG 동작 등 세부 사항은 [docs/dags.md](docs/dags.md)를 참고하세요.

---

## 🏗️ 2. 전체 시스템 아키텍처 (Summary)
```text
[External]                 [Service Layer: MySQL (OLTP)]                  [DW Layer: S3 (OLAP)]
Kiwoom API
│
├─ (평일 1회/08:10 KST) ───► d_market_asset_master (종목 마스터 갱신)
│
├─ (5분 주기)           ───► s_market_ohlcv (30분 Sliding Window Staging)
│                               │
│                               ▼ (UPSERT)
│                          s_market_ohlcv_history (48 히스토리)
│                               │
│                               ▼ Event-Driven Trigger
│                          [dag_trading] ──► 계좌 상태/밴드 분석/주문 집행
│
└─ (새벽 1회 배치)       ───► [Data Quality Gate (PySpark)]               ──► dw_trading_bronze (S3)
```
👉 자세한 데이터 아키텍처 및 네이밍 규칙은 [docs/architecture.md](docs/architecture.md)를 참고하세요.

---

## 💻 3. 기술 스택 & 저장소 구조

* **Language & Core**: Python 3.12, Apache Airflow 2.x, PySpark
* **Database & Storage**: MySQL 8.x (OLTP), AWS S3 (Data Lakehouse)
* **API & Infra**: Kiwoom REST API, Docker, AWS boto3(운영환경), Dev Containers(개발환경)

```text
.
├── .devcontainer/    # VS Code/Cursor Dev Container (Airflow UI 8080 포워딩)
├── configs/          # 환경별 런타임 프로파일 (secrets 경로, env, Airflow, target universe)
├── .secrets/         # 크리덴셜 (*.example.json 활용)
├── docs/             # 설계 문서, 상세 DAG 명세 등
├── infra/            # HTTP, MySQL, S3 등 공통 인프라 클라이언트
├── src/
│   ├── common/       # entity, config, logging 유틸
│   ├── market/       # [완료] dag_market 및 fetch/insert task
│   ├── trading/      # [예정] dag_trading 및 분석기/주문 로직
│   └── dw_migration/ # [예정] dag_dw_migration 및 DQ Gate/S3 이관
├── docker/           # Dockerfile, compose, entrypoint
├── scripts/          # 실행 헬퍼
└── requirements.txt
```
---
## 📚 4. 프로젝트 문서 목차
- 🏗️ 데이터 아키텍처 & 네이밍 컨벤션
- ⚡ 핵심 엔지니어링 의사결정 및 시스템 최적화 리포트
- 🔄 DAG별 세부 명세 및 Gate 정책
---
## ⚡ 5. 핵심 엔지니어링 의사결정 & 시스템 최적화 (Key Engineering Decisions)
### 1. API Rate Limit과 Airflow 지연 간의 연쇄 병목 차단
- **Problem & Constraint:** 초당 1회 Call Limit 제약 하에서 Airflow 지연 발생 시 연쇄 병목 발생.
- **Decision:** Catchup 순차 실행 대신 Dynamic Sliding Window(최신 30분) + UPSERT 기법 채택.
- **Result:** 정시 실행률 확보 및 과거 갭 데이터 자가 복구 구현.

### 2. 단일 종목 Timeout 시 전체 배치 중단 방지
- **Problem & Constraint:** 200개 종목 수집 중 1개 Read Timeout 시 전체 Task Failure 발생.
- **Decision:** API Client 레벨 지수 백오프 Retry 도입 및 실패 종목 Skip 구조 설계.
- **Result:** 파이프라인 가용성 극대화 및 연쇄 장애 차단.

👉 문제 원인 및 코드 레벨의 해결 과정은 [docs/engineering_decisions.md](docs/engineering_decisions.md)에서 확인할 수 있습니다.

---
## 🚀 6. 시작하기
### 사전 요구사항

- Docker (Dev Container 사용 시)
- MySQL 8.x
- Kiwoom Open API 앱 키 (`KIWOOM_APP_KEY`, `KIWOOM_SECRET_KEY`)

### 1. 클론

```bash
git clone <repository-url>
cd investbot
pip install -r requirements.txt
```

### 2. Secrets 설정

example 파일을 복사한 뒤 값을 채웁니다.

```bash
mkdir -p .secrets/dev/debug
cp .secrets/mysql.example.json .secrets/dev/debug/mysql.json
cp .secrets/broker.example.json .secrets/dev/debug/broker.json
```

| 파일 | 용도 |
|---|---|
| `mysql.json` | MySQL 접속 정보 |
| `broker.json` | Kiwoom API (`base_url`, `app_key`, `app_secret`, `account_no`) |

### 3. Dev Container (권장)

VS Code / Cursor에서 **Reopen in Container**로 `.devcontainer/devcontainer.json` 환경을 엽니다.  
포트 `8080`(Airflow UI)이 자동 포워딩됩니다.

### 4. Docker Compose

```bash
cp docker/.env.example .env
docker compose -f docker/docker-compose.yml up --build
```

### 설정 (`configs/`)

환경별 JSON 프로파일을 `INVESTBOT_CONFIG`로 지정해 한 번에 로드합니다.

```bash
export INVESTBOT_CONFIG=configs/dev/debug.json
```

로드되는 항목:

- `environment` — 공통 env 변수
- `environment_from_secrets` — secret JSON 필드를 env로 매핑
- `target_universe` — OHLCV 수집 대상 종목 코드
- `airflow` — `AIRFLOW_HOME`, DAGs folder 등

### Airflow 로컬 실행

`configs/dev/debug.json`의 Airflow env를 적용한 뒤 standalone을 기동합니다.

```bash
export INVESTBOT_CONFIG=configs/dev/debug.json

# config → os.environ 반영
python -c "from src.common.utils.config import init_runtime_config; init_runtime_config('configs/dev/debug.json')"

mkdir -p data/airflow
airflow db migrate
airflow standalone
```

- UI: http://localhost:8080
- DAGs folder: `src/` (`AIRFLOW__CORE__DAGS_FOLDER`)
- `AIRFLOW_HOME`: `data/airflow/` (로컬 생성, gitignore)

