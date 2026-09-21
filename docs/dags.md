# 🔄 DAG 상세 명세 및 스케줄링 정책

## 1. `dag_market` (마켓 데이터 공급)

* **스케줄**: `*/5 8-15 * * 1-5` (평일 08:00~15:55 KST, 5분 슬롯)
* **Task Workflow**:
```text
gate_fetch_d_market_asset_master → fetch_d_market_asset_master
gate_market_window → gate_asset_master_ready → fetch_s_market_ohlcv → insert_s_market_ohlcv_history
```
### Task 상세 역할
| Task | 설명 |
| :--- | :--- |
| `fetch_d_market_asset_master` | Kiwoom 종목 마스터 수집 → `d_market_asset_master` (TRUNCATE+INSERT) |
| `fetch_s_market_ohlcv` | 슬롯 기준 최신 7개 5분봉 fetch → `s_market_ohlcv` (Staging & Trading master) |
| `insert_s_market_ohlcv_history` | Staging → `s_market_ohlcv_history` UPSERT (일별 파티션) |

### Gate 정책 (KST, `logical_date` 기준)

| Gate | 통과 조건 | skip 사유 |
| :--- | :--- | :--- |
| `gate_fetch_d_market_asset_master` | 거래일 & (08:10 정규 실행 또는 08:15~08:55 당일 미갱신 시 재시도) | `not_a_trading_day`, `missing_calendar_row`, `outside_asset_master_window`, `asset_master_already_updated_today` |
| `gate_market_window` | 거래일 & 09:00~15:30, 15:21부터 15:29(동호가) 제외 | `not_a_trading_day`, `missing_calendar_row`, `outside_market_window`, `closing_duplicate_window_1521_1529` |
| `gate_asset_master_ready` | 09:00 이후 & 당일 `d_market_asset_master` 갱신 완료 | `before_ohlcv_window_0900`, `asset_master_stale` |

* 거래일 판단(`not_a_trading_day`)은 `weekday()` 계산이 아니라 `d_market_calendar`
  테이블 조회 기준이다. 주말·법정공휴일·대체공휴일·KRX 연말 폐장(12/31)을
  하나의 판단 기준으로 통합한다.
* `d_market_calendar`는 `dag_market`이 채우지 않는다. `exchange_calendars`
  (XKRX)로 계산해 `scripts/load_d_market_calendar.py`를 연 1회 또는 임시공휴일
  발표 시 수동 실행해 채운다. 해당 날짜 행이 없으면 게이트는
  `missing_calendar_row`로 skip하며, 이는 재적재가 필요하다는 신호다.

---

## 2. `dag_dw_migration` (DW 이관 — MySQL → S3 Bronze)

* **스케줄**: `40 15 * * 1-5` (평일 15:40 KST — 장 마감 직후)
* **Task Workflow**:
```text
gate_trading_day → extract_s_market_ohlcv_history → gate_dq_s_market_ohlcv_history
  → convert_s_market_ohlcv_history → upload_s_market_ohlcv_history
  → gate_glue_registration_s_market_ohlcv_history → register_glue_partition_s_market_ohlcv_history
upload_s_market_ohlcv_history → purge_s_market_ohlcv_history
```

### Task 상세 역할

| Task | 설명 |
| :--- | :--- |
| `extract_s_market_ohlcv_history` | ``source_table``(기본 `s_market_ohlcv_history`)에서 당일 `partition_date` 구간 추출 → 로컬 Parquet |
| `gate_dq_s_market_ohlcv_history` | pandas DQ Gate (Null/중복/범위/시간 갭). 실패 시 downstream 차단 |
| `convert_s_market_ohlcv_history` | pyarrow 스키마 정규화 → `data.parquet` + S3 manifest |
| `upload_s_market_ohlcv_history` | 1일 1 PUT |
| `register_glue_partition_s_market_ohlcv_history` | Glue Catalog |
| `purge_s_market_ohlcv_history` | S3 이관 완료한 ``partition_date`` 파티션만 MySQL `DROP PARTITION` |

### Gate 정책 (KST, `logical_date` 기준)

| Gate | 통과 조건 | skip 사유 |
| :--- | :--- | :--- |
| `gate_trading_day` | 거래일 (`d_market_calendar.is_market_open`) | `not_a_trading_day`, `missing_calendar_row`, `missing_logical_date` |
| `gate_dq_s_market_ohlcv_history` | 추출 Parquet DQ 통과 | skip이 아닌 **태스크 failed**로 처리(재시도 없음): `missing_extracted_parquet`, `DQ Gate failed: ...` |
| `gate_glue_registration_s_market_ohlcv_history` | `run_glue_registration=true` | `glue_registration_disabled_by_config` |

* DQ Gate는 PySpark가 아닌 **pandas**로 구현 (1일 배치 규모 고려).
* S3/Glue 파티션 키는 `partition_year → partition_month → partition_date` 3단 계층. parquet는 하루 1파일.
* devcontainer에서는 **moto_server** (`http://moto:5000`)로 S3 업로드·Glue 파티션 등록까지 E2E 검증 가능.
* E2E 테스트는 `s_market_ohlcv_history_test` 테이블에서 실행되며 실 데이터(`s_market_ohlcv_history`)는 전혀 조회/수정되지 않음.
* ⚠️ **운영 주의사항**: `INVESTBOT_CONFIG`는 DAG import 시점에 1회 로드 및 고정되며 실 데이터 `s_market_ohlcv_history`를 바라보는 라이브 config로 기동된 Airflow에서는 이 DAG를 수동 트리거할 경우 실 데이터 파티션을 그대로 `DROP`할 수 있음.
* 안전한 검증 절차: ① `export INVESTBOT_CONFIG=configs/dev/e2e.json` 후 Airflow를 기동 → ② `python scripts/seed_e2e_test_data.py`로 더미 데이터 생성 → ③ Airflow UI에서 `dag_dw_migration`을 수동 트리거.