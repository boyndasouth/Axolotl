"""Run the orders pipeline once and report what happened.

    python -m pipeline.run --source api
    python -m pipeline.run --source file:data/incoming/orders.json --json

With --json, the run report is printed to stdout as JSON. The monitor agent
runs this as a subprocess inside the working directory it is responsible for.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
import uuid

import pandera.pandas as pa

from pipeline import fingerprint, ingest
from pipeline.config import MIN_ROWS, ROOT, load_mappings
from pipeline.load import load_orders
from pipeline.logging_setup import get_logger
from pipeline.schemas import OrdersSchema
from pipeline.transforms.orders import transform_orders


def _summarize_schema_errors(err: pa.errors.SchemaErrors) -> list[dict]:
    fc = err.failure_cases
    out = []
    for (column, check), grp in fc.groupby(["column", "check"], dropna=False):
        out.append({
            "column": None if column != column else column,   # NaN -> None
            "check": str(check),
            "count": int(len(grp)),
            "examples": [str(x) for x in grp["failure_case"].head(5).tolist()],
        })
    return out


def run(source: str, load: bool = True) -> dict:
    log = get_logger()
    run_id = time.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    t0 = time.time()
    report = {"run_id": run_id, "source": source, "status": "ok", "rows_in": 0,
              "rows_out": 0, "raw_snapshot": None, "schema_diff": None,
              "error": None, "failures": [], "failed_check": None}
    log.info("run_started", run_id=run_id, source=source)

    try:
        records = ingest.load_records(source)
    except Exception as e:  # upstream unreachable
        report.update(status="failed", failed_check="ingest",
                      error={"type": type(e).__name__, "message": str(e)[:500]})
        log.error("ingest_failed", run_id=run_id, **report["error"])
        return report

    report["rows_in"] = len(records)
    report["raw_snapshot"] = str(ingest.snapshot(records, run_id))
    fp = fingerprint.fingerprint(records)
    report["schema_diff"] = fingerprint.diff(fingerprint.load_snapshot(), fp)
    if not fingerprint.is_empty(report["schema_diff"]):
        log.warning("schema_changed", run_id=run_id, diff=report["schema_diff"])

    if len(records) < MIN_ROWS:
        report.update(status="failed", failed_check="min_rows")
        log.error("too_few_rows", run_id=run_id, rows_in=len(records), min_rows=MIN_ROWS)
        return report

    try:
        df = transform_orders(records, load_mappings())
        OrdersSchema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        report.update(status="failed", failed_check="schema_validation",
                      failures=_summarize_schema_errors(e))
        log.error("validation_failed", run_id=run_id, failures=report["failures"])
        return report
    except Exception as e:
        # keep only frames from this project: library internals don't help diagnosis
        frames = [f for f in traceback.extract_tb(e.__traceback__)
                  if str(ROOT) in f.filename and "site-packages" not in f.filename]
        tb = "".join(traceback.format_list(frames[-5:]))
        report.update(status="failed", failed_check="transform",
                      error={"type": type(e).__name__, "message": str(e)[:500],
                             "traceback": tb})
        log.error("transform_failed", run_id=run_id, **report["error"])
        return report

    report["rows_out"] = load_orders(df) if load else len(df)
    fingerprint.save_snapshot(fp)   # this batch is the new known-good shape
    log.info("run_succeeded", run_id=run_id, rows_in=len(records),
             rows_out=report["rows_out"], seconds=round(time.time() - t0, 3))
    return report


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", default="api")
    p.add_argument("--no-load", action="store_true")
    p.add_argument("--json", action="store_true")
    a = p.parse_args()
    report = run(a.source, load=not a.no_load)
    if a.json:
        print(json.dumps(report, default=str))
    else:
        print(f"[{report['status']}] run {report['run_id']}: {report['rows_in']} in, "
              f"{report['rows_out']} out" + (f", failed at {report['failed_check']}"
                                             if report["failed_check"] else ""))
    sys.exit(0 if report["status"] == "ok" else 1)


if __name__ == "__main__":
    main()
