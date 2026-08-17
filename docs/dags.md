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