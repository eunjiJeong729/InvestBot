# 🏗️ InvestBot 데이터 아키텍처 & 네이밍 컨벤션 명세서

본 문서는 **InvestBot** 시스템의 하이브리드 데이터 파이프라인, 메달리온 아키텍처 적용 기준, 물리적 DB 설계 전략 및 데이터 네이밍 컨벤션을 다룹니다.

---

## 📌 1. 데이터 아키텍처 개요 (Overview)

InvestBot은 **실시간 매매 집행의 정시성**과 **대용량 과거 시계열 데이터 분석의 효율성**을 동시에 달성하기 위해 **하이브리드 Dual-Layer 아키텍처**를 채택했습니다.

* **실시간 영역 (Service Layer - MySQL OLTP)**:
  * 5분 주기로 동작하며 최신 24시간~48시간 동안의 데이터만 슬라이딩 윈도우 방식으로 핸들링합니다.
  * DB 메모리 및 I/O 부하를 최적화하여 트레이딩 런타임 상태 판단 및 주문 실행을 타이트하게 수행합니다.
* **분석 영역 (Data Lakehouse Layer - AWS S3 OLAP)**:
  * 하루 1회 장 마감 후 배치 작업을 통해 PySpark 기반의 **Data Quality Gate**를 거친 데이터만 이관합니다.
  * 장기 시계열 데이터를 영구 보존하고 전략 성과 분석 및 AI 모델 학습을 위한 데이터 마트를 구축합니다.

---

## 🏛️ 2. 메달리온 아키텍처 & 저장소 구분 (Medallion Architecture)

| 저장소 (Storage) | DB / Layer | 데이터 성격 | 주요 역할 |
| :--- | :--- | :--- | :--- |
| **MySQL (RDS)** | `trading_prod` | OLTP / Staging | 실시간 시세 수집, 30분 슬라이딩 윈도우, 트레이딩 시그널 및 체결 상태 관리 |
| **AWS S3** | `dw_trading_bronze` | OLAP (Raw Lake) | DQ Gate 검증을 통과한 원천 데이터의 일별 파티션 영구 보존 |
| **AWS S3** | `dw_trading_gold` | OLAP (Data Mart) | 당일 체결 내역과 수집 시세 데이터를 결합한 스타 스키마 기반의 성과 분석 마트 |

---

## 🔄 3. 데이터 생애주기 & 파이프라인 단계 (Data Lifecycle & Flow)
```text
[Step 1: 수집] ──► [Step 2: 판단/분기] ──► [Step 3: 밴드 분석] ──► [Step 4: 시그널] ──► [Step 5: 실행]
 Kiwoom API        계좌 상태 API           AI 밴드 범위 연산        BUY/SELL 생성       주문 체결 UPDATE
│                                                                                      │
▼                                                                                      ▼
d_market_asset_master                                                            s_trading_signal
s_market_ohlcv
│
▼ (UPSERT)
s_market_ohlcv_history (MySQL 48hr Window)
│
├─ (Daily 1회 배치) ──► [Step 6: DQ Gate (PySpark)] ──► dw_trading_bronze (S3 적재)
└─ (ETL 성공 확인 후)──► [Step 7: MySQL Purge] (48시간 이전 데이터 삭제)
```

### 파이프라인 단계별 세부 명세

| Step | 레이어 | 주기 | 주요 작업 내용 | 대상 테이블 / API |
| :---: | :--- | :---: | :--- | :--- |
| **1** | 서비스 (MySQL) | 5분 | Kiwoom API 종목 마스터 및 30분 슬라이딩 윈도우(7개 5분봉) OHLCV 수집 | `d_market_asset_master`<br>`s_market_ohlcv` |
| **2** | 서비스 (MySQL) | 5분 | 계좌 잔고/주문 가능 금액 조회 및 추가 매수 필요 여부 판단 | 계좌 API (Kiwoom REST) |
| **3** | 서비스 (MySQL) | 5분 | [조건 A: YES] 전체 자산 AI 밴드 분석<br>[조건 B: NO] 보유 자산 밴드 범위 이탈 체크 | `f_trading_asset_band` |
| **4** | 서비스 (MySQL) | 5분 | 조건 만족 시 매수/매도 시그널 생성 및 DB 적재 (Insert) | `s_trading_signal` |
| **5** | 서비스 (MySQL) | 5분 | 증권사 주문 API 호출 및 주문 체결 결과 반영 (Update) | `s_trading_signal` |
| **6** | DW (S3) | 48시간 | PySpark 로컬 연산을 통한 데이터 누락/중복/오류 사전 검증 (DQ Gate) | `dw_trading_bronze` |
| **7** | 서비스 (MySQL) | 48시간 | S3 이관 완결 확인 후 MySQL 내 48시간 이전 파티션/데이터 Purge | `s_market_ohlcv_history` |

---

## 🏷️ 4. 데이터 네이밍 컨벤션 (Naming Conventions)

> 아래 접두사/접미사 체계는 Kimball 방법론의 Dimension/Fact/Staging 개념을 참고하여, 본 프로젝트의 서비스 구조(마켓 데이터 수집·트레이딩·DW 이관)에 맞게 직접 설계했습니다.

### 1) 테이블 네이밍 규칙 (`[접두사]_[서비스명]_[본문]_[이력(선택)]`)

#### 🔹 접두사 (Prefix): 데이터 성격 및 역할
* **`d_` (Dimension - 기준 정보)**: 변경 주기가 길고 분석/조회의 기준이 되는 마스터 데이터
  * *예시*: `d_market_asset_master` (종목 마스터), `d_market_calendar` (거래일 캘린더)
* **`s_` (Stage / Source - 원천 로그)**: 외부 API나 시스템 내부 이벤트에서 발생한 원천 데이터
  * *예시*: `s_market_ohlcv` (시세 Staging), `s_trading_signal` (주문 시그널)
* **`f_` (Fact - 집계/가공 데이터)**: 비즈니스 로직 연산을 거쳐 가공된 핵심 지표 데이터
  * *예시*: `f_trading_asset_band` (AI 밴드 분석 결과)

#### 🔹 접미사 (Suffix): 데이터 적재 행위 및 시점 상태
* **원형 (Overwrite/Latest)**: 별도의 접미사를 붙이지 않으며, 현재 시점의 최신 상태만 유지/갱신함을 의미합니다.
  * *예시*: `f_trading_asset_band`
* **`_history` (Insert/UPSERT)**: 최신 상태 테이블명 뒤에 명시하며, 시계열 스냅샷 및 이력이 누적되는 테이블임을 나타냅니다.
  * *예시*: `s_market_ohlcv_history`

---

### 2) Airflow DAG 및 Task 네이밍 규칙

* **DAG ID**: 서비스 도메인 단위로 명확하게 1:1 매핑하여 명명합니다.
  * `dag_market`: 마켓 데이터 수집 및 마스터 관리 서비스
  * `dag_trading`: 전략 연산, 밴드 분석 및 매매 주문 집행 서비스
  * `dag_dw_migration`: Data Quality Gate 검증 및 S3 이관 서비스
* **Task ID**: `[행위]_[테이블명 또는 목적]` 형태의 소문자 snake_case를 사용합니다.
  * *예시*: `gate_market_window`, `fetch_s_market_ohlcv`, `insert_s_market_ohlcv_history`

---

## 🛡️ 6. 데이터 품질 검증 관문 & Purge 정책 (DQ Gate & Purge)
### 1) PySpark 기반 Data Quality Gate 검증 항목
S3 DW 적재 전 배치 파이프라인에서 다음 4가지 핵심 조건 검증을 통과해야만 DW 이관 및 RDS Purge가 진행됩니다.
- Null Check: asset_code, market_time, close_price 등 필수 필드 결측치 존재 여부
- Duplicate Check: 동일 (asset_type, asset_code, market_time) 복합키 중복 적재 여부
- Range Check: close_price <= 0 또는 volume < 0 등 금융 데이터 유효 범위 이탈 여부
- Gap Check: 거래 시간 내 5분 단위 연속성 유실(Gap) 비율 모니터링

### 2) RDS Purge 정책
- 보존 기간: 최근 48시간 (2일치) 데이터만 유지
- 실행 방식: s_market_ohlcv_history 파티션 중 48시간 이전 파티션 ALTER TABLE ... DROP PARTITION 수행으로 DB I/O 부하 및 테이블 락 최소화.