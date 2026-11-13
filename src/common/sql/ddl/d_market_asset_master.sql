CREATE TABLE IF NOT EXISTS `d_market_asset_master` (
  `asset_type` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '자산 유형 (STOCK, BOND, ETF)',
  `asset_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '종목 코드 (예: 005930, SPY)',
  `asset_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '종목명 (한글/영문)',
  `is_tradable` tinyint(1) NOT NULL DEFAULT '1' COMMENT '거래 가능 여부 (1: 가능, 0: 중지)',
  `is_target` tinyint(1) NOT NULL DEFAULT '0' COMMENT '거래 대상 여부 (1: 대상, 0: 비대상)',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '데이터 생성 일시',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '데이터 수정 일시',
  PRIMARY KEY (`asset_type`,`asset_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='기준 정보 마스터';
