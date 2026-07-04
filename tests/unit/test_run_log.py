from procedure.config import IcebergSyncConfig
from procedure.run_log import run_log_relation

from .test_config import base_config


def test_run_log_relation_uses_procedure_schema_when_enabled():
    config = IcebergSyncConfig.from_dict(base_config())

    assert run_log_relation(config) == '"DB"."UTIL"."ICEBERG_SYNC_RUN_LOG"'


def test_run_log_relation_can_be_disabled():
    config = IcebergSyncConfig.from_dict(
        base_config(deployment={"procedure_database": "DB", "procedure_schema": "UTIL", "run_log_enabled": False})
    )

    assert run_log_relation(config) is None
