# 🏛️ 핵심 엔지니어링 의사결정 & 시스템 최적화 리포트 (Key Engineering Decisions)

> 본 문서는 금융 API의 환경적 제약(Rate Limit, 네트워크 불확실성)과 오케스트레이터(Airflow)의 지연 환경 속에서 데이터 정합성과 시스템 가용성을 확보하기 위해 내린 **핵심 엔지니어링 의사결정 및 트레이드오프**를 기록한 문서입니다.

---

## 1. 장 마감 동호가 구간 API Quota 낭비 방지 및 데이터 멱등성 구조 설계

* **Context & Constraints (배경 및 제약 조건)**
  * 장 마감 동호가 체결 구간(15:20 ~ 15:30) 특성상 신규 5분 봉 데이터가 생성되지 않음.
  * 단순 스케줄링 시 동일한 데이터에 대한 불필요한 API Call이 지속되어 API Daily Quota가 오남용되고 DB 적재 시 Primary Key 중복 에러가 발생하는 리스크 존재.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * 과거 Run 순차 재실행(Backfill) 시에도 안전하게 동작하도록 **오케스트레이터 레이어와 데이터베이스 레이어의 2단계 방어막** 구조를 채택.

* **Technical Solution (기술적 해결책)**
  1. **1차 방어 (Application Level)**: Airflow `logical_date` 기준 15:21 ~ 15:29 구간 수집을 명시적으로 Skip하여 불필요한 API 호출 차단.
  2. **2차 방어 (Storage Level)**: DB 적재 쿼리에 `WHERE NOT EXISTS` 기법을 적용하여 중복 키 진입을 원천 방지.

* **Impact & Reliability (성과 및 시스템 안정성)**
  * 불필요한 API Call Quota 방어 및 파이프라인 장애율 개선.
  * 과거 지점 재작업(Backfill) 시 동일 데이터 중복 산출을 차단하는 멱등성 확보.

---

## 2. API Rate Limit(모의투자 기준 초당 1회) 및 스케줄러 지연 환경에서의 자가 복구 아키텍처

* **Context & Constraints (배경 및 제약 조건)**
  * **API Rate Limit**: 키움 REST API는 TR(조회/주문/환전)별로 초당 호출 한도가 상이하며
    국내주식 실전투자 기준 조회/주문 TR은 초당 5회임. 현재 개발·운영 중인 계좌는
    모의투자 계좌로 모의투자는 이보다 보수적인 초당 1회 제한이 적용됨.
    `KiwoomCredentials.max_rps` 기본값(5.0)은 추후 실전 계좌 전환을 염두에 두고
    설정.
  * **Tight Execution Window**: 200개 종목 수집 시 1개 Run당 약 2분 30초 소요되어
    5분 주기 실행 시간의 50%를 점유.
  * Airflow 스케줄러 부하로 지연 발생 시 과거 밀린 Run들이 순차 실행되면
    정작 매매 신호를 발행해야 하는 최신 Run이 대기열에 들어가 딜레마 발생.
  * 1번 항목의 `WHERE NOT EXISTS` 적재 방식은 단일 최신 봉만 1회 수집하던 구조에서는
    문제없었지만 아래 Dynamic Sliding Window 도입으로 동일 봉이 여러 Run에 걸쳐
    반복 수집되면서 변경되는 분봉 확정값(종가/거래량)을
    DB에 반영하지 못하는 문제 발생.
    
* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * Past Run을 순차적으로 다 실행하는 방식을 **Dynamic Sliding Window** 방식으로 개선.
  * 지연 후 처음 복구된 단 1번의 최신 Run이 과거의 누락 데이터까지 한 번에 복구하도록 설계하여 연쇄 병목을 완전히 차단함.

* **Technical Solution (기술적 해결책)**
  1. **30분 Dynamic Sliding Window (7개 분봉 일괄 수집)**: API 1회 호출 시 최신 30분 분량(7개 분봉)을 동시 수집하도록 파이프라인 개편. 스케줄러 지연으로 일부 Run이 누락되어도 최신 Run 1번으로 30분 내 발생한 데이터 갭을 자동 자가 복구.
  2. **UPSERT 이관**: 히스토리 테이블 적재 방식을 기존 `WHERE NOT EXISTS`에서
     `UPSERT`(`ON DUPLICATE KEY UPDATE`)로 교체. Dynamic Sliding Window 구조상 매 Run마다 가장 최신 봉이 아직
     마감 전(진행 중) 상태로 처음 수집될 수 있음. 이때 `WHERE NOT EXISTS`나 단순
     `INSERT`를 쓰면 이 미완성 값이 DB에 그대로 고정되고 이후 Run에서 해당 봉이
     마감돼도 다시 갱신되지 않음.

  3. **실측 검증(2026-08-25, 200종목, 14:36~15:22 KST)**: Run 단위로 값 변경 시점을 확인.

     | 구간 | 내용 | 건수 |
     |---|---|---|
     | 처음 수집한 Run → 바로 다음 Run | 봉이 마감 전(진행 중) 상태로 처음 수집됐다가 바로 다음 Run에서 확정값으로 갱신 | 1,538건 (open 0건) |
     | 그 이후 Run들 | 확정값을 받은 뒤 Run이 몇 번 더 지나도 재수집(188,196회) 시 값 변경 | 0건 |

     즉 값 변경은 "처음 수집한 Run → 바로 다음 Run" 구간에서 최대 1회만 일어나고 그 이후 Run에서는 변경 없음. 실제 UPSERT 대상인 5분 집계봉 기준으로도
     (13,808건 중 374건 동일 패턴) 같은 결과가 확인됨.
     
* **Impact & Reliability (성과 및 시스템 안정성)**
  * Past Run 연쇄 집행으로 인한 Queue 병목 차단 및 실시간 정시 실행률 100% 달성.
  * 스케줄러 장애 복구 후 수동 개입 없는 자가 복구 완료 및 데이터 누락 방지.

---

## 3. 네트워크 불확실성에 대응하는 내결함성 및 Partial Success 적재 구조 구현

* **Context & Constraints (배경 및 제약 조건)**
  * 200개 종목 OHLCV 수집 중 단 1개 종목이라도 API 서버 통신 불안정으로 Read Timeout 발생 시 예외가 상위 Task로 전파되어 전체 배치 수집 작업이 전면 중단되는 문제 발생.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * Airflow Task 레벨의 Retry 방식은 200개 종목 전체에 대한 API 재요청을 유발하여 API Provider 단에 순간 트래픽 부하를 가중시킴.
  * 따라서 **API Client 레벨 Micro-retry + 부분 성공 적재 및 Error 격리** 아키텍처로 전환.

* **Technical Solution (기술적 해결책)**
  1. **Exponential Backoff Micro-retry**: API Client 단에서 HTTP/Read Timeout 발생 시 지수 백오프 전략을 기반으로 최대 5회 미세 재시도 수행.
  2. **Error Isolation & Partial Success**: 최종 재시도 실패 종목은 `fetch_errors` 메타 데이터 레이어로 Skip하고 예외 전파를 차단, 정상 수집된 나머지 종목은 Staging Table 적재.

* **Impact & Reliability (성과 및 시스템 안정성)**
  * 단일 예외 전파에 따른 전체 파이프라인 중단 리스크 차단 및 시스템 내결함성 강화.
  * 일시적 외부 네트워크 장애 환경에서도 수집 가용성 극대화.
---
## 4. 서비스 확장을 고려한 모듈 분리 기준 수립

* **Context & Constraints (배경 및 제약 조건)**
  * 후속 서비스(`dag_trading`, `dag_dw_migration`) 추가를 앞두고 서비스 간 반복될 코드 패턴(스냅샷 적재, 파티션 관리)이 `dag_market` 내에서 이미 식별됨.
  * Task를 잘게 쪼갤 경우 대용량 fetch 결과(200종목 × 7분봉) 전달을 위한 XCom이 필요해져 API Rate Limit 제약 하 재시도 비용이 커지고 반대로 모든 로직을 한 파일에 두면 향후 서비스 간 로직 중복이 우려됨.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * Task 경계는 "부분 실패 시 함께 재시도되어야 하는 범위"로 유지, 코드 재사용은 "실제 재사용 근거가 있는가"를 기준으로 별도 판단하여 분리.

* **Technical Solution (기술적 해결책)**
  1. 실체(API 오케스트레이션)와 불일치하던 `sql/` 폴더를 `tasks/`로 개명, `sql/`은 정적 SQL만 남김.
  2. 재사용 근거가 확인된 TRUNCATE+INSERT, 파티션 네이밍 로직만 `common/utils/`로 공통화.
  3. Task 내 SQL은 "정적 쿼리로 완전히 표현 가능한가"를 기준으로 분리. UPSERT는 조건 분기 없이 항상 동일하게 실행되므로 `.sql`로 완전히 분리해 task가 직접 로드하도록 구성한 반면, 파티션 존재 여부 확인 후 필요한 것만 추가하는 로직은 실행할 쿼리 자체가 매번 달라지는 절차적 판단이라 `.sql`로 표현 불가하여 task 내부에 유지.

* **Impact & Reliability (성과 및 시스템 안정성)**
  * Task 재시도 단위 유지로 안전한 재시도 확보, 근거 없는 추상화 없이 코드 중복 최소화 및 후속 서비스 개발 시 재구현 비용 절감.
---
## 5. 1분봉 대신 5분 집계봉 채택 및 결측 처리 기준

* **Context & Constraints (배경 및 제약 조건)**
  * 1분봉은 노이즈가 많아 후속 분석(AI 밴드 분석 등) 모델 성능에 불리함.
  * 5분 구간 내 일부 1분봉이 존재하지 않는 종목(저유동성)이 발생.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * Kiwoom에서 받은 1분봉을 5분 구간 단위로 집계(open=구간 첫값, high/low=구간 극값, close=구간 마지막값, volume=합산)하여 노이즈를 줄인 정제 데이터로 적재.
  * "5분 평균봉"이 아닌 "체결 발생분만으로 구성된 압축 구간"이므로, 구간 내 일부 1분봉이 없어도 존재하는 값만으로 집계하고 별도 보정 없이 진행.

* **Impact & Reliability (성과 및 시스템 안정성)**
  * 노이즈가 완화된 5분봉으로 후속 분석 모델의 입력 품질 향상.
  * 저유동성 종목의 결측 1분봉으로 인한 불필요한 수집 실패/skip 없이 안정적으로 데이터 적재.

---
## 6. pendulum 객체에 stdlib ZoneInfo를 혼용하여 발생한 게이트 슬롯 오판

* **Context & Constraints (배경 및 제약 조건)**
  * `dag_market`의 게이트가 `outside_asset_master_window`로 판정하며 `fetch_d_market_asset_master`를 연속으로 skip, 실제 종목 마스터 갱신이 누락됨.
  * 원인 추적 결과 Airflow `logical_date`(`pendulum.DateTime`)를 KST로 변환하는 슬롯 계산 로직이 실제 시각과 다른 값을 산출하고 있었음.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * 최초에는 `astimezone()` 호출 자체가 오프셋을 반영하지 않는 것으로 판단하여 `.timestamp()`를 경유해 stdlib `datetime`으로 완전히 바꿔치기하는
    워크어라운드(`to_aware_utc()`)를 공용 유틸(`src/common/utils/util.py`)로 도입 검토.
  * 재현 스크립트로 재검증한 결과 실제 원인이 다르다는 것을 확인: `astimezone()` 단독 호출 결과값은 정상으로 보였지만 pendulum 객체가 외부(stdlib) tzinfo를 빌려 쓴 상태라 내부적으로 불안정했고 그 결과에 `+ timedelta(...)`를 체이닝하는 시점에만 tzinfo가 소실되는 결함이었음.
  * 또한 `to_aware_utc()`를 공용 유틸로 배치하려던 최초 결정도 실제 소비처가 market 도메인 파일 두 곳(`dag_market.py`, `fetch_s_market_ohlcv.py`)뿐이라 프로젝트에서 이미 세워둔 "재사용 근거 없는 공용 유틸 추출 금지(YAGNI)" 원칙에 위배된다고 생각하고 재검토함.
  * 최종적으로 원인을 pendulum 자체 API(`in_timezone()`)로 해결하면서 워크어라운드 함수 자체가 불필요해져 원칙 위배도 자연 해소됨.

* **Technical Solution (기술적 해결책)**
  1. `raw.astimezone(ZoneInfo(...)) + timedelta(...)` → `pendulum.instance(raw).in_timezone(...) + timedelta(...)`로 전환.
     `in_timezone()`은 pendulum 자체 타임존 표현을 사용하므로 이후 `timedelta` 연산을 이어붙여도 tzinfo가 안정적으로 유지됨.
  2. `raw`가 naive datetime으로 들어오는 경로에 대한 방어 추가: naive는 UTC로 명시적으로 tzinfo를 붙인 뒤(`slot_dt`) `pendulum.instance(slot_dt)` 로 변환. (`pendulum`가 naive를 로컬 시스템 타임존으로 오인할 수 있어 명시적 UTC 부착)

* **Impact & Reliability (성과 및 시스템 안정성)**
  * 별도 워크어라운드 유틸 없이 원인을 구조적으로 제거하여 코드 복잡도 증가 없이 해결.

---

## 7. 거래일 판단 방식 전환: `weekday()` 체크에서 `d_market_calendar` 스냅샷 기반으로

* **Context & Constraints (배경 및 제약 조건)**
  * 게이트가 `slot_kst.weekday() >= 5`로 주말만 걸렀고, 법정 공휴일·대체공휴일·
    KRX 연말 폐장(12/31)은 걸러내지 못해 휴장일에도 fetch task가 정상 실행을
    시도함.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * `exchange_calendars`(XKRX 캘린더)로 거래일 여부·개장 시각을 계산해
    `d_market_calendar`에 스냅샷 적재하고 게이트는 이 테이블만 조회하도록 전환.
  * 적재 주기(연 1회, 불규칙한 임시공휴일 발표 대응)가 `dag_market`의 5분
    스케줄과 맞지 않아 Airflow DAG으로 만들지 않고 `scripts/`의 수동 CLI로
    분리. 반복 스케줄링·재시도 오케스트레이션이 필요 없는 관리성 작업에
    Airflow를 쓰는 건 과함(YAGNI)으로 판단.
  * 적재 범위를 "올해"가 아닌 작년~내후년(4개년)으로 잡아서 연말/연초 경계에서
    다음 연도 데이터가 없어 게이트가 잘못 판단하는 엣지케이스를 사전 차단.

* **Technical Solution (기술적 해결책)**
  1. `build_calendar_rows()`가 XKRX 세션을 날짜별로 계산해
     `(market_date, is_market_open, market_open_time)` row 생성.
  2. 기존 `db_snapshot.replace_snapshot()`을 재사용해 TRUNCATE+INSERT (신규
     유틸 없이 재사용 근거 있는 기존 함수만 사용).
  3. 게이트의 `weekday() >= 5`를 `_reject_non_trading_day()`로 교체, 주말·
     공휴일·연말폐장을 `not_a_trading_day` 하나로 통합 판단. 캘린더 행 누락은
     `missing_calendar_row`로 별도 식별해 재적재 필요 신호로 활용.

* **Impact & Reliability (성과 및 시스템 안정성)**
  * 공휴일·대체공휴일·연말 폐장에도 fetch task가 실행 시도되던 문제 제거.
  * 별도 오케스트레이션 없이 결정론적 데이터를 스냅샷으로만 관리해 히스토리
    적재 부담 없이 최소 구성 유지.
    
---
## 8. Task Class 전환에 따른 모듈 조직 및 네이밍 원칙 재정립

* **Context & Constraints (배경 및 제약 조건)**
  * Airflow task를 함수형에서 ABC 기반 클래스(`Task`)로 전환하면서 `tasks/` 하위에 흩어져 있던 helper 모듈(`_ohlcv_aggregation.py`, `_kiwoom_api.py`, `_market_calendar.py`)의 위치와 존재 이유를 재검토할 필요가 발생.
  * 4번 항목("서비스 확장을 고려한 모듈 분리 기준")에서 세운 "재사용 근거 없는 분리 금지" 원칙과 별도로 남아 있던 "테스트 용이성을 위한 aggregation 로직 분리" pending 항목이 서로 충돌.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  1. **Task-local 로직 병합 우선**: 소비처가 단일 task 파일 하나뿐인 로직은 별도 파일로 분리하지 않고 해당 task 파일 내부로 병합한다. 분리는 "실제 재사용 근거"가 생긴 시점에만 수행한다. 테스트 용이성만으로는 분리 근거가 되지 않는다 — 이는 4번 항목 원칙의 연장이며 기존에 세워뒀던 "테스트 용이성을 위한 aggregation 분리" pending 항목은 폐기한다.
  2. **재사용 근거의 범위 확장**: "확인된 재사용 근거"는 실제 호출 코드가 존재하는 경우뿐 아니라, `architecture.md`에 설계상 확정된 향후 소비처도 포함한다. Kiwoom API 클라이언트는 `dag_trading`(계좌 조회, 주문 집행)에서 사용이 architecture.md Step 2/5에 이미 문서화되어 있으므로 `dag_market` 구현 완료 시점에 공통 모듈로 격상 가능. 단, 이 확장은 막연한 예상 재사용까지 허용하지 않는다.
  3. **common vs infra 위치**: 소비처가 `src/` 내부로 한정되는 한 `common/`과 `infra/`의 구분은 실익이 없어 강제하지 않는다. 단, 향후 `src/` 밖(예: 별도 배치 스크립트, 다른 레포)에서 소비하는 모듈이 생기면 그때 `infra/`(외부 API/DB 클라이언트) vs `common/`(entity, config, logging 등 도메인 무관 유틸)의 기존 구분을 따른다.

* **Technical Solution (기술적 해결책)**
  1. `_ohlcv_aggregation.py`의 순수 함수들을 `fetch_s_market_ohlcv.py`로 병합 (유일 소비처).
  2. `_kiwoom_api.py` → `src/common/kiwoom_api.py`로 격상 (`dag_trading`의 확정된 미래 소비 근거).
  3. `_market_calendar.py`의 적재 로직은 `scripts/load_d_market_calendar.py`로 병합(수동 CLI 스크립트가 유일 소비처), 조회 로직(`_is_market_open`)은 `dag_market.py` 내부 게이트 헬퍼로 이동.
  4. 함수 기반 -> 클래스 패턴으로 통일.

* **Impact & Reliability (성과 및 시스템 안정성)**
  * Task 클래스 전환과 무관하게 "재사용 근거 기반 분리" 원칙이 일관되게 유지됨을 재확인.
  * 설계 판단 기준 재확립.

---
## 9. 서버 아키텍처 재검토: RDS 분리 배치 vs EC2 내 통합 배치

* **Context & Constraints (배경 및 제약 조건)**
  * 최초 설계는 Amazon RDS for MySQL을 별도로 두고 EC2에는 Airflow만 올리는 구조를 검토.
  * 실제 OLTP 데이터 스케일 산정 결과: 대상 200종목 × 5분봉(09:00~15:30, 78봉/일), `s_market_ohlcv`는 30분 슬라이딩 윈도우 유지로 약 1,400행, `s_market_ohlcv_history`는 48시간(2일) 보존 기준 약 31,200행 — 합산 스토리지 요구량이 10MB 미만 수준으로 극히 작음을 확인.
  * RDS는 컴퓨팅(인스턴스)과 스토리지가 별도 과금되는 구조이며 프리티어(db.t4g.micro) 기준으로도 EC2 t3 계열과 이중으로 비용이 발생.
  * 장 운영시간(08:00~15:30, 월 약 21거래일 기준 약 157.5시간)만 가동한다는 전제로 RDS 분리안과 EC2 통합안을 컴퓨팅+스토리지 합산 비용으로 비교.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * 데이터 스케일이 작아 RDS의 관리 편의성(자동 백업, 장애조치)이 주는 이점 대비 이중 과금 구조로 인한 비용 손해가 더 크다고 판단.
  * RDS 분리(EC2 t3.micro + RDS db.t4g.micro) 대비 EC2 통합(MySQL 직접 설치, t3.small)이 동급 RAM 구성에서 약 절반 수준의 비용으로 산정됨.
  * 단, EC2 통합 시 인스턴스 RAM이 Claude Code, Airflow, MySQL이 동시에 상주해야 하는 병목 지점이 되므로 t3.micro(1GB) 보다 t3.small(2GB)을 실사용 한계선 후보로 보고 실측 테스트 진행.

* **Technical Solution (기술적 해결책)**
  1. MySQL을 EC2 인스턴스 내부에 직접 설치하여 로컬 MySQL에 localhost로 접속하도록 구성.
  2. t3.micro / t3.small 두 사양으로 Airflow 태스크(특히 `fetch_s_market_ohlcv`, `insert_s_market_ohlcv_history`) 및 `airflow standalone`(scheduler+webserver+triggerer) 동시 구동 상태에서 `vmstat`으로 유휴 메모리를 실측.
  3. 실측 결과를 근거로 t3.small(2GB)을 최소 운영 사양으로 확정. t3.micro(1GB)는 이 구성에서 실사용 불가로 판단해 제외.

* **Impact & Reliability (성과 및 시스템 안정성)**
  * 데이터 스케일 대비 과도한 관리형 서비스(RDS) 채택을 지양해 비용 구조를 단순화.
  * 사양 결정을 이론적 계산이 아닌 실측 기반으로 진행하여 추후 종목 수/봉 간격 확장 시에도 재현 가능한 사양 산정 근거를 남김.
  * `vmstat` 실측 결과, 유휴 상태 free 메모리가 두 시나리오 모두 t3.micro(1GB)에서는 안정권 미달임을 확인 — t3.small(2GB)을 최종 최소 사양으로 확정.
---
## 10. Airflow triggerer 프로세스 제외를 통한 메모리 추가 확보

* **Context & Constraints (배경 및 제약 조건)**
  * 9번 항목에서 t3.small(2GB)을 최소 사양으로 확정했으나 MySQL+Airflow+개발환경 접속이 동시에 상주하는 구조라 추가적인 메모리 여유 확보가 필요.
  * `airflow standalone`은 scheduler+webserver+triggerer를 함께 기동하는데 triggerer는 deferrable operator(예: `deferrable=True` 센서)를 사용할 때만 필요한 프로세스.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * 현재 `dag_market`은 `PythonOperator`/`ShortCircuitOperator`만 사용하여 deferrable 오퍼레이터가 전혀 없음을 코드 전수 확인 완료. triggerer가 상시 유휴 상태임.
  * `dag_trading`(Datasets 기반 트리거 예정), `dag_dw_migration`(외부 이벤트 대기 없음)도 deferrable 오퍼레이터가 필요 없는 구조로 설계되어 있어 triggerer 제외가 향후 서비스 확장 이후에도 유효할 것으로 판단.

* **Technical Solution (기술적 해결책)**
  1. `scripts/run_airflow_ui.py`에 `--no-triggerer` 옵션 추가, 지정 시 scheduler와 webserver만 별도 프로세스로 기동.

* **Impact & Reliability (성과 및 시스템 안정성)**
  * `vmstat` 실측 결과, triggerer 포함 시 유휴 상태 free 메모리 약 75MB, 제외 시 약 192MB로 확인 — 약 117MB의 추가 여유 확보.
  * 추후 deferrable 오퍼레이터 도입이 실제로 필요해지는 시점에 재검토 조건으로 명시.

---

## 11. DW 이관 DQ Gate: PySpark → pandas 전환

* **Context & Constraints (배경 및 제약 조건)**
  * 초반 설계 시 DQ Gate를 PySpark 기반으로 명시했으나 실제 처리 대상(1일치 `s_market_ohlcv_history`, 200종목×78봉 ≈ 3~4MB)은 PySpark 로컬 모드 구동 자체의 JVM 오버헤드(최소 1.5~2GB)에 비해 지나치게 작음을 확인.
  * dag_dw_migration 배치가 EC2 인스턴스(t3.micro/t3.small, 1~2GB RAM) 위에서 MySQL·Airflow와 동시에 실행되는 구조이므로 배치 실행 그 순간만 대형 인스턴스로 리사이즈하는 방안도 검토했으나 Stop→Modify→Start 비용 대비 이득이 작음을 확인.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * 현재 데이터 스케일에서는 PySpark 보다 실제 운영 비용·복잡도 관점에서 pandas + pyarrow로 전환을 결정.
  * 데이터 스케일(종목 수 등)이 확장되어 PySpark의 분산처리 이점이 실익으로 전환되는 시점에 재검토하기로 결정.

* **Technical Solution (기술적 해결책)**
  1. Null/중복/Range/Gap 4개 DQ 체크 항목을 pandas 연산으로 구현.
  2. Parquet 변환은 `pyarrow` 엔진을 사용해 `partition_date` 폴더 구조 방식 적용.
  3. 인스턴스 리사이즈 없이 동일 사양(EC2 t3 계열)에서 배치 실행이 가능하도록 유지.

* **Impact & Reliability (성과 및 시스템 안정성)**
  * 불필요한 엔진 오버헤드 제거로 EC2 사양을 낮게 유지하면서도 dag_dw_migration 실행 가능.
  * 향후 데이터 스케일 확장 시 PySpark 재도입 판단 기준(처리 데이터량 대 구동 오버헤드 비교)을 명시.

---

## 12. DW 이관 배치 스케줄: 새벽 1회 → 장마감 직후로 변경

* **Context & Constraints (배경 및 제약 조건)**
  * 최초 설계는 dag_dw_migration을 "Daily 새벽 1회 배치"로 설계.
  * 그러나 EC2 가동 시간을 장 운영시간(08:00~15:30)에 맞춰 최소화하는 비용 전략과 새벽 배치 스케줄이 충돌 — 새벽에 배치를 돌리려면 별도 시간대에 인스턴스를 추가로 기동해야하므로 관리 포인트와 비용 증가.
  * `s_market_ohlcv_history`는 장 마감(15:30) 이후 더 이상 변경되지 않는 당일 데이터이므로 새벽까지 대기해야 할 데이터 정합성 상의 이유가 없음을 확인.

* **Engineering Decision & Trade-off (의사결정 및 트레이드오프)**
  * dag_dw_migration 배치를 장마감 직후(당일 장 운영 슬롯 종료 시점)로 변경.

* **Technical Solution (기술적 해결책)**
  1. MySQL 추출 → DQ Gate(pandas) → Parquet 변환 → S3 PUT → Glue 등록 → MySQL Purge 순서를 장마감 후 단일 실행 흐름으로 구성.
  2. 배치 완료 확인 후 EC2 인스턴스를 중지하는 방식으로 운영.

* **Impact & Reliability (성과 및 시스템 안정성)**
  * 새벽 시간대 인스턴스 추가 기동 없이 하루 가동 시간대를 하나로 통합. EC2 컴퓨팅 비용 최소화.
  * 데이터 정합성(당일 데이터 불변 시점)과 비용 최적화가 동시에 만족되는 스케줄로 재설계.