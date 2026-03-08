#!/usr/bin/env python3
"""
download_bdc_pnw.py — FCC BDC data download for Pacific Northwest states.

Downloads Fixed Broadband Location Coverage CSVs for ALL fiber providers
in WA (53), OR (41), ID (16), MT (30).

Two modes:
  --discover    List all providers filing in target states, show fiber BSL counts
  --download    Download CSVs for target providers (or all if none specified)

Uses curl for HTTP (Python urllib times out on FCC's API).

Usage:
  python3 download_bdc_pnw.py --discover              # find all providers
  python3 download_bdc_pnw.py --download               # download all
  python3 download_bdc_pnw.py --download --force        # re-download everything
"""

import argparse
import json
import os
import subprocess
import sys
import time
import zipfile
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlencode

# ============================================
# CONFIGURATION
# ============================================

BASE_URL = 'https://bdc.fcc.gov'
AS_OF_DATE = '2025-06-30'  # Latest availability filing (J25)
RATE_LIMIT_DELAY = 6.5  # seconds between API calls

# Target states: Pacific Northwest (Ziply's 4-state footprint)
TARGET_STATES = {
    '53': 'Washington',
    '41': 'Oregon',
    '16': 'Idaho',
    '30': 'Montana',
}

# Known major providers (for labeling; discovery will find ALL providers)
KNOWN_PROVIDERS = {
    '131461': 'Ziply Fiber',
    '130077': 'Lumen/CenturyLink',
    '130235': 'Charter/Spectrum',
    '130403': 'Astound/Wave',
    '131425': 'Verizon',
    '130360': 'Cox',
    '130258': 'Frontier',
    '130536': 'Comcast',
    '130335': 'Consolidated/Fidium',
}

SCRIPT_DIR = Path(__file__).parent
FCC_DATA_DIR = SCRIPT_DIR / 'fcc_data'
ENV_FILE = SCRIPT_DIR / '.env'
DISCOVERY_FILE = SCRIPT_DIR / 'fcc_data' / 'pnw_providers_discovery.json'


# ============================================
# CREDENTIALS
# ============================================

def load_credentials(args):
    """Load FCC credentials from args or .env file."""
    username = getattr(args, 'username', None)
    token = getattr(args, 'token', None)

    if not username or not token:
        if ENV_FILE.exists():
            with open(ENV_FILE) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('#') or '=' not in line:
                        continue
                    key, val = line.split('=', 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if key == 'FCC_USERNAME':
                        username = val
                    elif key == 'FCC_TOKEN':
                        token = val

    if not username or not token:
        print("[ERROR] FCC credentials not found.")
        print(f"  Create {ENV_FILE} with:")
        print("    FCC_USERNAME=your_username")
        print("    FCC_TOKEN=your_api_token")
        sys.exit(1)

    return username, token


# ============================================
# API CALLS (using curl)
# ============================================

def api_get(url, username, token, timeout=180):
    """GET JSON from FCC API using curl."""
    result = subprocess.run(
        ['curl', '-s', '--max-time', str(timeout),
         '-H', f'username: {username}',
         '-H', f'hash_value: {token}',
         url],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed (exit {result.returncode}): {result.stderr}")
    return json.loads(result.stdout)


def api_download(url, username, token, output_path, timeout=600):
    """Download file from FCC API using curl."""
    header_file = str(output_path) + '.headers'
    result = subprocess.run(
        ['curl', '-s', '--max-time', str(timeout),
         '-H', f'username: {username}',
         '-H', f'hash_value: {token}',
         '-D', header_file,
         '-o', str(output_path),
         url],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed (exit {result.returncode}): {result.stderr}")

    actual_path = output_path
    if os.path.exists(header_file):
        with open(header_file) as f:
            for line in f:
                if 'filename=' in line.lower():
                    fname = line.split('filename=')[1].strip().strip('"').strip("'").strip()
                    if fname:
                        actual_path = output_path.parent / fname
                        if actual_path != output_path:
                            os.rename(output_path, actual_path)
                    break
        os.remove(header_file)

    size = actual_path.stat().st_size if actual_path.exists() else 0
    return actual_path, size


# ============================================
# DISCOVERY MODE
# ============================================

def discover_providers(username, token):
    """
    List ALL Fixed Broadband providers filing in target PNW states.
    Returns a dict of provider_id -> {name, states, files, total_records}
    """
    print(f"[1/2] Querying all Fixed Broadband filings for as_of_date={AS_OF_DATE}...")

    params = urlencode({
        'category': 'Provider',
        'subcategory': 'Location Coverage',
        'technology_type': 'Fixed Broadband',
    })
    url = f'{BASE_URL}/api/public/map/downloads/listAvailabilityData/{AS_OF_DATE}?{params}'

    data = api_get(url, username, token)

    if data.get('status') != 'successful':
        print(f"[ERROR] API returned: {data.get('message', 'Unknown error')}")
        sys.exit(1)

    total = data.get('result_count', 0)
    all_files = data.get('data', [])
    print(f"  Total Fixed Broadband files nationwide: {total}")

    # Filter to target states
    pnw_files = [
        item for item in all_files
        if item.get('state_fips', '') in TARGET_STATES
    ]
    print(f"  Files in PNW states (WA/OR/ID/MT): {len(pnw_files)}")

    # Group by provider
    providers = defaultdict(lambda: {
        'name': '',
        'states': {},
        'files': [],
        'total_records': 0,
    })

    for item in pnw_files:
        pid = item.get('provider_id', '')
        pname = item.get('provider_name', '') or item.get('brand_name', '') or KNOWN_PROVIDERS.get(pid, f'Unknown ({pid})')
        state_fips = item.get('state_fips', '')
        state_name = item.get('state_name', TARGET_STATES.get(state_fips, '?'))
        records = int(item.get('record_count', 0) or 0)

        providers[pid]['name'] = pname
        providers[pid]['states'][state_fips] = {
            'name': state_name,
            'records': records,
            'file_id': item.get('file_id'),
            'file_name': item.get('file_name', ''),
        }
        providers[pid]['files'].append(item)
        providers[pid]['total_records'] += records

    print(f"  Unique providers in PNW: {len(providers)}")

    # Sort by total records descending
    sorted_providers = sorted(providers.items(), key=lambda x: x[1]['total_records'], reverse=True)

    print(f"\n{'='*90}")
    print(f"ALL PROVIDERS IN WA/OR/ID/MT (sorted by total BSL records)")
    print(f"{'='*90}")
    print(f"{'ID':>8}  {'Provider Name':<45}  {'States':<12}  {'Records':>10}  {'Candidate?'}")
    print(f"{'-'*90}")

    consolidation_candidates = []

    for pid, pdata in sorted_providers:
        states_str = ','.join(sorted(pdata['states'].keys()))
        state_abbrs = ','.join(TARGET_STATES.get(s, s)[:2] for s in sorted(pdata['states'].keys()))
        is_candidate = pdata['total_records'] < 50000
        candidate_flag = '  << CANDIDATE' if is_candidate else ''

        pname_display = pdata['name']
        total_recs = pdata['total_records']
        print(f"  {pid:>6}  {pname_display:<45}  {state_abbrs:<12}  {total_recs:>10,}{candidate_flag}")

        if is_candidate and pdata['total_records'] > 0:
            consolidation_candidates.append({
                'provider_id': pid,
                'name': pdata['name'],
                'states': {k: v['name'] for k, v in pdata['states'].items()},
                'total_records': pdata['total_records'],
                'state_records': {v['name']: v['records'] for k, v in pdata['states'].items()},
            })

    print(f"\n{'='*90}")
    print(f"CONSOLIDATION CANDIDATES (<50K fiber BSLs): {len(consolidation_candidates)}")
    print(f"{'='*90}")

    for c in consolidation_candidates:
        states_detail = ', '.join(f"{s}: {r:,}" for s, r in c['state_records'].items())
        print(f"  {c['provider_id']:>6}  {c['name']:<40}  {c['total_records']:>8,}  ({states_detail})")

    # Save discovery results
    FCC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    discovery = {
        'as_of_date': AS_OF_DATE,
        'target_states': TARGET_STATES,
        'total_providers': len(providers),
        'all_providers': {pid: {
            'name': pdata['name'],
            'total_records': pdata['total_records'],
            'states': {k: {'name': v['name'], 'records': v['records']} for k, v in pdata['states'].items()},
        } for pid, pdata in sorted_providers},
        'consolidation_candidates': consolidation_candidates,
    }

    with open(DISCOVERY_FILE, 'w') as f:
        json.dump(discovery, f, indent=2)
    print(f"\n  Discovery saved: {DISCOVERY_FILE}")

    return providers, pnw_files


# ============================================
# DOWNLOAD MODE
# ============================================

def download_files(target_files, username, token, force=False):
    """Download all target files via API."""
    FCC_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not force:
        needed = []
        already_have = []
        for item in target_files:
            state = item.get('state_fips', '')
            pid = item.get('provider_id', '')
            existing = list(FCC_DATA_DIR.glob(f"bdc_{state}_{pid}_*.csv"))
            if existing:
                already_have.append(item)
            else:
                needed.append(item)
        if already_have:
            print(f"\n  Already have {len(already_have)} files, skipping.")
    else:
        needed = target_files

    if not needed:
        print("\n  All files already downloaded!")
        return

    est_min = len(needed) * RATE_LIMIT_DELAY / 60
    print(f"\n  Downloading {len(needed)} files...")
    print(f"  Rate limit: 1 request every {RATE_LIMIT_DELAY}s (~{est_min:.0f} min total)")

    downloaded = 0
    failed = []

    for i, item in enumerate(needed):
        file_id = item.get('file_id')
        state = item.get('state_name', '?')
        state_fips = item.get('state_fips', '??')
        pid = item.get('provider_id', '')
        pname = KNOWN_PROVIDERS.get(pid, item.get('provider_name', pid))
        records = item.get('record_count', '?')

        print(f"  [{i+1}/{len(needed)}] {state} ({state_fips}) / {pname} ({records} records)...", end=' ', flush=True)

        try:
            output_name = f"bdc_{state_fips}_{pid}_download.zip"
            url = f'{BASE_URL}/api/public/map/downloads/downloadFile/availability/{file_id}'
            target_path, size_bytes = api_download(
                url, username, token,
                FCC_DATA_DIR / output_name,
            )
            size_mb = size_bytes / 1e6
            print(f"OK ({size_mb:.1f} MB) -> {target_path.name}")
            downloaded += 1
        except Exception as e:
            print(f"FAILED: {e}")
            failed.append((state, pname, str(e)))

        if i < len(needed) - 1:
            time.sleep(RATE_LIMIT_DELAY)

    print(f"\n  Done. Downloaded {downloaded}/{len(needed)} files.")
    if failed:
        print(f"  Failed ({len(failed)}):")
        for state, pname, err in failed:
            print(f"    - {state} / {pname}: {err}")

    # Unzip
    unzip_all()


def unzip_all():
    """Unzip all ZIP files in fcc_data/."""
    zips = list(FCC_DATA_DIR.glob('*.zip'))
    if not zips:
        return
    print(f"\n  Unzipping {len(zips)} files...")
    for zf in zips:
        try:
            with zipfile.ZipFile(zf, 'r') as z:
                z.extractall(FCC_DATA_DIR)
            print(f"    OK: {zf.name}")
            zf.unlink()
        except Exception as e:
            print(f"    FAILED: {zf.name}: {e}")


# ============================================
# CLI
# ============================================

def main():
    global AS_OF_DATE

    parser = argparse.ArgumentParser(description='FCC BDC downloader for PNW states')
    parser.add_argument('--username', help='FCC username')
    parser.add_argument('--token', help='FCC API token')
    parser.add_argument('--discover', action='store_true', help='Discover all providers in PNW states')
    parser.add_argument('--download', action='store_true', help='Download BDC CSVs')
    parser.add_argument('--force', action='store_true', help='Re-download even if files exist')
    parser.add_argument('--providers', nargs='*', help='Specific provider IDs to download (default: all)')
    parser.add_argument('--as-of-date', default=AS_OF_DATE, help=f'As-of date (default: {AS_OF_DATE})')
    args = parser.parse_args()

    AS_OF_DATE = args.as_of_date

    print("=" * 70)
    print("FCC BDC — Pacific Northwest Provider Discovery & Download")
    print(f"Target states: {', '.join(f'{v} ({k})' for k, v in sorted(TARGET_STATES.items()))}")
    print(f"As-of date: {AS_OF_DATE}")
    print("=" * 70)

    username, token = load_credentials(args)
    print(f"  Authenticated as: {username}")

    if not args.discover and not args.download:
        print("\n  Specify --discover or --download (or both)")
        parser.print_help()
        return

    providers = None
    pnw_files = None

    if args.discover:
        providers, pnw_files = discover_providers(username, token)

    if args.download:
        if pnw_files is None:
            # Need to fetch the file list
            providers, pnw_files = discover_providers(username, token)

        # Filter to specific providers if requested
        if args.providers:
            pnw_files = [f for f in pnw_files if f.get('provider_id', '') in args.providers]
            print(f"\n  Filtered to {len(pnw_files)} files for providers: {', '.join(args.providers)}")

        download_files(pnw_files, username, token, force=args.force)

    print("\n" + "=" * 70)
    print("DONE!")
    print("=" * 70)


if __name__ == '__main__':
    main()
