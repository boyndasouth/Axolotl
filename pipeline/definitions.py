"""Optional Dagster definitions for the BATCH pipeline (pip install -r requirements-dagster.txt).

    dagster dev -f pipeline/definitions.py        # UI on http://localhost:3000

The project now runs as a stream (stream/), which processes records as they arrive
and has its own detector, so there is no schedule here any more: polling every few
minutes is the opposite of real time. This file stays for manual batch runs and
backfills; a failed run still hands the incident to the agent.
"""


import os
import subprocess
import sys

import duckdb
from dagster import (AssetCheckResult, AssetExecutionContext, Definitions, MetadataValue,
                     RunFailureSensorContext, asset, asset_check, define_asset_job,
                     run_failure_sensor)

from pipeline.config import ROOT
from pipeline.load import WAREHOUSE
from pipeline.run import run as run_pipeline

SOURCE = os.environ.get("SHP_SOURCE", "api")


@asset(description="Orders from the upstream API, transformed and validated against the contract.")
def orders_clean(context: AssetExecutionContext) -> None:
    report = run_pipeline(SOURCE)
    context.add_output_metadata({
        "run_id": report["run_id"], "rows_in": report["rows_in"], "rows_out": report["rows_out"],
        "schema_diff": MetadataValue.json(report["schema_diff"] or {}),
    })
    if report["status"] != "ok":
        raise RuntimeError(f"pipeline failed at {report['failed_check']}: "
                           f"{report['error'] or report['failures']}")


@asset_check(asset=orders_clean, description="The warehouse table is non-empty.")
def orders_clean_not_empty() -> AssetCheckResult:
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        n = con.execute("SELECT count(*) FROM orders_clean").fetchone()[0]
    finally:
        con.close()
    return AssetCheckResult(passed=n > 0, metadata={"rows": n})


orders_job = define_asset_job("orders_job", selection=[orders_clean])


@run_failure_sensor(monitored_jobs=[orders_job], description="Hand failed runs to the self-healing agent.")
def heal_on_failure(context: RunFailureSensorContext) -> None:
    context.log.info(f"run {context.dagster_run.run_id} failed; starting the self-healing agent")
    subprocess.Popen([sys.executable, "-m", "agent.cli", "run", "--source", SOURCE], cwd=ROOT)


defs = Definitions(assets=[orders_clean], asset_checks=[orders_clean_not_empty],
                   jobs=[orders_job], sensors=[heal_on_failure])
