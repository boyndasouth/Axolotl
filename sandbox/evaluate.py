"""Runs INSIDE the sandbox. Evaluates one copy of the pipeline code on a set of inputs.

    python evaluate.py --code /work/patched --manifest /work/manifest.json

Prints one JSON line to stdout: a list of per-input results.
Standard library + pandas/pandera/yaml only; nothing from the host project. Which
transform, contract and mappings to use comes from the manifest's "spec" (written by
the host from healer.yaml), so this works for any pipeline, not just the demo.
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback


def _compare(actual, expected_records, float_decimals: int = 6) -> int:
    """Rows present in one table but not the other (0 means an exact match).
    Floats are compared rounded to `float_decimals`; everything else as text."""
    import pandas as pd

    exp = pd.DataFrame.from_records(expected_records)
    act = actual.copy().reset_index(drop=True)
    if list(act.columns) != list(exp.columns):
        return max(len(act), len(exp)) or 1
    if act.empty and exp.empty:
        return 0
    floats = [c for c in exp.columns if pd.api.types.is_float_dtype(exp[c]) or pd.api.types.is_float_dtype(act[c])]
    for df in (act, exp):
        for c in df.columns:
            if c in floats:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype(float).round(float_decimals)
            else:
                df[c] = df[c].astype(str)
    merged = act.merge(exp, how="outer", indicator=True)
    dupes = abs(int(act.duplicated().sum()) - int(exp.duplicated().sum()))
    return int((merged["_merge"] != "both").sum()) + dupes


def _load(code_dir: str, entrypoint: str, name: str):
    import importlib.util
    import os

    path, attr = entrypoint.rsplit(":", 1)
    spec = importlib.util.spec_from_file_location(name, os.path.join(code_dir, path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, attr)


DEMO_SPEC = {"transform": "pipeline/transforms/orders.py:transform_orders",
             "contract": "pipeline/schemas.py:OrdersSchema", "mappings": "pipeline/mappings.yaml",
             "float_decimals": 2}


def evaluate(code_dir: str, inputs: list[dict], spec: dict | None = None) -> list[dict]:
    import os

    import yaml

    sys.path.insert(0, code_dir)
    import pandera.pandas as pa

    spec = spec or DEMO_SPEC
    transform = _load(code_dir, spec["transform"], "shp_eval_transform")
    contract = _load(code_dir, spec["contract"], "shp_eval_contract")
    mappings = {}
    if spec.get("mappings"):
        with open(os.path.join(code_dir, spec["mappings"]), encoding="utf-8") as f:
            mappings = yaml.safe_load(f) or {}
    decimals = int(spec.get("float_decimals", 6))

    results = []
    for item in inputs:
        res = {"name": item["name"], "ok": False, "rows_in": 0, "rows_out": 0,
               "diff_rows": None, "error": None, "schema_failures": []}
        try:
            with open(item["raw"], encoding="utf-8") as f:
                records = json.load(f)
            res["rows_in"] = len(records)
            df = transform(records, mappings)
            res["rows_out"] = len(df)
            contract.validate(df, lazy=True)
            res["ok"] = True
            if item.get("expected"):
                with open(item["expected"], encoding="utf-8") as f:
                    res["diff_rows"] = _compare(df, json.load(f), decimals)
        except pa.errors.SchemaErrors as e:
            fc = e.failure_cases
            res["error"] = "schema validation failed"
            res["schema_failures"] = [
                {"column": str(col), "check": str(chk), "count": int(len(g)),
                 "examples": [str(x) for x in g["failure_case"].head(3)]}
                for (col, chk), g in fc.groupby(["column", "check"], dropna=False)]
        except Exception as e:
            res["error"] = f"{type(e).__name__}: {str(e)[:300]}"
            res["traceback"] = "".join(traceback.format_exception(e)[-3:])[-800:]
        results.append(res)
    return results


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--code", required=True)
    p.add_argument("--manifest", required=True)
    a = p.parse_args()
    with open(a.manifest, encoding="utf-8") as f:
        manifest = json.load(f)
    print("@@RESULT@@" + json.dumps(evaluate(a.code, manifest["inputs"], manifest.get("spec"))))
