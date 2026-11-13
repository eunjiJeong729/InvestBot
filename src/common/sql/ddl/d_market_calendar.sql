CREATE TABLE IF NOT EXISTS `d_market_calendar` (
  `market_date` date NOT NULL COMMENT '거래일자 (YYYY-MM-DD)',
  `is_market_open` tinyint(1) NOT NULL DEFAULT '0' COMMENT '개장 여부 (1: 개장, 0: 휴장)',
  `market_open_time` time DEFAULT NULL COMMENT '장 시작 시각 (휴장일은 NULL, 예: 09:00)',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '데이터 생성 일시',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '데이터 수정 일시',
  PRIMARY KEY (`market_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='마켓 거래소 개장/휴장 스케줄 마스터';
