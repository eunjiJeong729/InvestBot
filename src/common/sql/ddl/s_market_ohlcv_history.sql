CREATE TABLE IF NOT EXISTS `s_market_ohlcv_history` (
  `asset_type` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '자산 유형 (STOCK, BOND, ETF)',
  `asset_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '종목 코드',
  `open_price` decimal(16,2) NOT NULL COMMENT '시가',
  `high_price` decimal(16,2) NOT NULL COMMENT '고가',
  `low_price` decimal(16,2) NOT NULL COMMENT '저가',
  `close_price` decimal(16,2) NOT NULL COMMENT '종가',
  `volume` bigint NOT NULL COMMENT '거래량',
  `market_time` datetime NOT NULL COMMENT '시장 가격 시간 (체결 기준시)',
  `partition_date` date NOT NULL COMMENT '파티셔닝 키 (조회 최적화용 날짜 데이터)',
  `partition_hour` tinyint NOT NULL COMMENT '시간대 (0 ~ 23)',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '데이터 수집 일시',
  PRIMARY KEY (`asset_type`,`asset_code`,`market_time`,`partition_date`,`partition_hour`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='누적 시장 시세 이력'
PARTITION BY RANGE COLUMNS(partition_date) (
    PARTITION p_init VALUES LESS THAN ('2000-01-01')
);
