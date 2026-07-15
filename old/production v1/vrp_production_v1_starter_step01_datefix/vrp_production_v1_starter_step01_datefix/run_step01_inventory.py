from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running without package installation.
THIS_DIR = Path(__file__).resolve().parent
SRC_DIR = THIS_DIR / "vrp_prod" / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from vrp_prod.production.inventory import build_inventory, write_inventory_report


def main() -> int:
    parser = argparse.ArgumentParser(description="VRP Production v1 Step 01 inventory check")
    parser.add_argument("--project-root", default=".", help="VRP project root. Default: current directory")
    parser.add_argument("--as-of", default=None, help="As-of date, YYYY-MM-DD. Default: today")
    args = parser.parse_args()

    report = build_inventory(project_root=args.project_root, as_of=args.as_of)
    outputs = write_inventory_report(report, project_root=args.project_root)

    print(json.dumps({
        "latest_completed_eod_date": report["latest_completed_eod_date"],
        "latest_term_structure_date": report["repaired_term_structure_check"].get("latest_date"),
        "latest_chain_cache_date": report["raw_chain_cache_inventory"].get("latest_trade_date"),
        "missing_eod_term_structure_dates": report["missing_eod_term_structure_dates"],
        "reports": outputs,
    }, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
