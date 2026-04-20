#!/usr/bin/env python3
"""
EVA Market Report Runner — called by Node.js via execSync.

Modes:
  python3 run_report.py analyse  <csv_path> <args_json_path>
  python3 run_report.py generate <csv_path> <pdf_output_path> <args_json_path>

All arguments are file paths or a mode string — no shell-escaping issues.
Outputs a single line of JSON to stdout; any Python errors go to stderr.
"""

import sys
import os
import json
import traceback

import numpy as np
import pandas as pd

# Market report generator lives one directory above scripts/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from market_report_generator import (
    parse_pm_transactions,
    analyse,
    generate_report,
)


# ── JSON serialiser that handles numpy / pandas types ───────────────────────
def _safe(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj) if not np.isnan(obj) else None
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    return str(obj)


def emit(data):
    print(json.dumps(data, default=_safe), flush=True)


# ── Mode: analyse ────────────────────────────────────────────────────────────
def mode_analyse(csv_path, args):
    """
    Parse the PM CSV and compute all metrics.
    Returns a flat dict for single reports, or { areas_data: [...] } for comparison.
    """
    communities = args.get('communities', [])
    report_type = args.get('report_type', 'single')

    try:
        txn_df = parse_pm_transactions(csv_path)
    except Exception as e:
        emit({'error': f'CSV parse failed: {e}'})
        sys.exit(1)

    rental_csv = args.get('rental_csv_path')
    if rental_csv and os.path.exists(rental_csv):
        try:
            from market_report_generator import parse_pm_rentals
            rental_df = parse_pm_rentals(rental_csv)
        except Exception as e:
            rental_df = pd.DataFrame()
            sys.stderr.write(f'Warning: could not parse rental CSV: {e}\n')
    else:
        rental_df = pd.DataFrame()

    if report_type == 'comparison' and len(communities) > 1:
        areas_data = []
        for community in communities:
            try:
                result = analyse(txn_df, rental_df, community)
                areas_data.append(result)
            except Exception as e:
                areas_data.append({'community': community, 'error': str(e)})
        emit({'report_type': 'comparison', 'areas_data': areas_data})
    else:
        community = communities[0] if communities else ''
        try:
            result = analyse(txn_df, rental_df, community)
            result['report_type'] = 'single'
            emit(result)
        except Exception as e:
            emit({'error': f'Analysis failed: {e}\n{traceback.format_exc()}'})
            sys.exit(1)


# ── Mode: generate ───────────────────────────────────────────────────────────
def mode_generate(csv_path, pdf_output_path, args):
    """
    Generate the full PDF report. `data` contains all pre-computed metrics
    AND Gemini narrative fields merged in by Node.js before calling this.
    """
    data = args.get('data', {})

    rental_csv = args.get('rental_csv_path')
    rental_csvs = [rental_csv] if rental_csv and os.path.exists(rental_csv) else []

    try:
        generate_report(
            output_path=pdf_output_path,
            data=data,
            txn_csvs=[csv_path],
            rental_csvs=rental_csvs,
        )
        emit({'success': True, 'output': pdf_output_path})
    except Exception as e:
        emit({'success': False, 'error': str(e), 'traceback': traceback.format_exc()})
        sys.exit(1)


# ── Entry point ──────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 3:
        emit({'error': 'Usage: run_report.py <mode> <csv_path> [pdf_output_path] <args_json_path>'})
        sys.exit(1)

    mode     = sys.argv[1]
    csv_path = sys.argv[2]

    if not os.path.exists(csv_path):
        emit({'error': f'CSV file not found: {csv_path}'})
        sys.exit(1)

    if mode == 'analyse':
        args_path = sys.argv[3] if len(sys.argv) > 3 else None
        args      = json.load(open(args_path)) if args_path and os.path.exists(args_path) else {}
        mode_analyse(csv_path, args)

    elif mode == 'generate':
        if len(sys.argv) < 5:
            emit({'error': 'generate mode requires: <csv_path> <pdf_output_path> <args_json_path>'})
            sys.exit(1)
        pdf_output_path = sys.argv[3]
        args_path       = sys.argv[4]
        args            = json.load(open(args_path)) if os.path.exists(args_path) else {}
        mode_generate(csv_path, pdf_output_path, args)

    else:
        emit({'error': f'Unknown mode: {mode}. Use "analyse" or "generate".'})
        sys.exit(1)


if __name__ == '__main__':
    main()
