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