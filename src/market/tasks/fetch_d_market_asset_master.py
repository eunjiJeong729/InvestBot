"""태스크: fetch_d_market_asset_master."""

from __future__ import annotations

from datetime import datetime

from infra.db.rdbms.mysql import MySQLClient
from src.common.entity import DMarketAssetMaster
from src.common.utils.config import load_mysql_config, load_target_universe
from src.common.utils.db_snapshot import replace_snapshot
from src.common.utils.task_log import log_task_run

from src.market.tasks._kiwoom_api import (
    KiwoomApi,
    KiwoomCredentials,
    parse_asset_code,
    parse_asset_name,
    parse_asset_type,
)

_TASK = "fetch_d_market_asset_master"


def run() -> int:
    """키움 자산 마스터를 조회해 d_market_asset_master를 덮어쓴다."""
    with log_task_run(_TASK) as logger:
        api = KiwoomApi(KiwoomCredentials.from_env())
        rows = api.fetch_asset_rows()
        now = datetime.now()
        target_universe = load_target_universe()
        logger.info(
            "fetched api rows=%d target_universe=%d",
            len(rows),
            len(target_universe),
        )

        entities: list[DMarketAssetMaster] = []
        skipped = 0
        for row in rows:
            asset_code = parse_asset_code(row)
            asset_name = parse_asset_name(row)
            if not asset_code or not asset_name:
                skipped += 1
                continue
            entities.append(
                DMarketAssetMaster(
                    asset_type=parse_asset_type(row),
                    asset_code=asset_code,
                    asset_name=asset_name,
                    is_tradable=1,
                    is_target=1 if asset_code in target_universe else 0,
                    created_at=now,
                    updated_at=now,
                )
            )

        if not entities:
            raise RuntimeError("No market assets fetched from Kiwoom API")

        target_count = sum(1 for entity in entities if entity.is_target == 1)
        logger.info(
            "prepared entities=%d skipped=%d is_target=1=%d",
            len(entities),
            skipped,
            target_count,
        )

        db = MySQLClient(load_mysql_config())
        replace_snapshot(
            db,
            table=DMarketAssetMaster.TABLE,
            columns=(
                "asset_type",
                "asset_code",
                "asset_name",
                "is_tradable",
                "is_target",
                "created_at",
                "updated_at",
            ),
            rows=[
                (
                    entity.asset_type,
                    entity.asset_code,
                    entity.asset_name,
                    entity.is_tradable,
                    entity.is_target,
                    entity.created_at,
                    entity.updated_at,
                )
                for entity in entities
            ],
        )

        logger.info("loaded table=%s rows=%d", DMarketAssetMaster.TABLE, len(entities))
        return len(entities)
