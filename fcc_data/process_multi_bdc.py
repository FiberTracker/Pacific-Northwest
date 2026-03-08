#!/usr/bin/env python3
"""
Process FCC BDC data for multiple Pacific Northwest fiber providers.
Aggregates BSLs to census block group level for map shading.
Fetches block group polygon boundaries from TIGERweb REST API.
Outputs: {provider}_bdc.js per provider (GeoJSON with block group polygons)

Providers:
  - Ziply Fiber / Northwest Fiber, LLC (420173): WA, OR, ID, MT
  - Lumen Technologies (130228): WA, OR, ID, MT
  - Hunter Communications (300066): OR, WA
  - Astound / Wave / Radiate Holdings (130079): OR, WA
  - TDS Telecom (131310): WA, OR, ID, MT

Usage:
  python process_multi_bdc.py                 # Process all configured providers
  python process_multi_bdc.py ziply lumen     # Process specific providers
  python process_multi_bdc.py --all           # Process ALL CSV files found in fcc_data/
"""

import csv
import glob
import json
import os
import sys
import urllib.request
import urllib.parse
import time
from collections import defaultdict

# --- PROVIDER CONFIGURATION ---
PROVIDERS = {
    'ziply': {
        'id': 420173,
        'brand': 'Northwest Fiber, LLC',
        'display': 'Ziply Fiber',
        'tech_filter': 50,  # Fiber only
        'states': ['53', '41', '16', '30'],  # WA, OR, ID, MT
        'js_const': 'ZIPLY_BDC_COVERAGE',
        'js_file': 'ziply_bdc.js',
        'color': '#22C55E',  # green
    },
    'lumen': {
        'id': 130228,
        'brand': 'Lumen Technologies',
        'display': 'Lumen Technologies',
        'tech_filter': 50,
        'states': ['53', '41', '16', '30'],
        'js_const': 'LUMEN_BDC_COVERAGE',
        'js_file': 'lumen_bdc.js',
        'color': '#3B82F6',  # blue
    },
    'hunter': {
        'id': 300066,
        'brand': 'Hunter Communications',
        'display': 'Hunter Communications',
        'tech_filter': 50,
        'states': ['41', '53'],  # OR, WA
        'js_const': 'HUNTER_BDC_COVERAGE',
        'js_file': 'hunter_bdc.js',
        'color': '#EF4444',  # red
    },
    'astound': {
        'id': 130079,
        'brand': 'Radiate Holdings',
        'display': 'Astound / Wave Broadband',
        'tech_filter': 50,
        'states': ['41', '53'],  # OR, WA
        'js_const': 'ASTOUND_BDC_COVERAGE',
        'js_file': 'astound_bdc.js',
        'color': '#F97316',  # orange
    },
    'tds': {
        'id': 131310,
        'brand': 'TDS Telecom',
        'display': 'TDS Telecom',
        'tech_filter': 50,
        'states': ['53', '41', '16', '30'],
        'js_const': 'TDS_BDC_COVERAGE',
        'js_file': 'tds_bdc.js',
        'color': '#14B8A6',  # teal
    },
}

# State FIPS codes for PNW
STATE_FIPS = {
    '53': 'WA',
    '41': 'OR',
    '16': 'ID',
    '30': 'MT',
}

# County FIPS to name mapping -- ALL counties for WA (39), OR (36), ID (44), MT (56)
COUNTY_NAMES = {
    # =========================================================================
    # WASHINGTON (53) - 39 counties
    # =========================================================================
    '53001': 'Adams',
    '53003': 'Asotin',
    '53005': 'Benton',
    '53007': 'Chelan',
    '53009': 'Clallam',
    '53011': 'Clark',
    '53013': 'Columbia',
    '53015': 'Cowlitz',
    '53017': 'Douglas',
    '53019': 'Ferry',
    '53021': 'Franklin',
    '53023': 'Garfield',
    '53025': 'Grant',
    '53027': 'Grays Harbor',
    '53029': 'Island',
    '53031': 'Jefferson',
    '53033': 'King',
    '53035': 'Kitsap',
    '53037': 'Kittitas',
    '53039': 'Klickitat',
    '53041': 'Lewis',
    '53043': 'Lincoln',
    '53045': 'Mason',
    '53047': 'Okanogan',
    '53049': 'Pacific',
    '53051': 'Pend Oreille',
    '53053': 'Pierce',
    '53055': 'San Juan',
    '53057': 'Skagit',
    '53059': 'Skamania',
    '53061': 'Snohomish',
    '53063': 'Spokane',
    '53065': 'Stevens',
    '53067': 'Thurston',
    '53069': 'Wahkiakum',
    '53071': 'Walla Walla',
    '53073': 'Whatcom',
    '53075': 'Whitman',
    '53077': 'Yakima',
    # =========================================================================
    # OREGON (41) - 36 counties
    # =========================================================================
    '41001': 'Baker',
    '41003': 'Benton',
    '41005': 'Clackamas',
    '41007': 'Clatsop',
    '41009': 'Columbia',
    '41011': 'Coos',
    '41013': 'Crook',
    '41015': 'Curry',
    '41017': 'Deschutes',
    '41019': 'Douglas',
    '41021': 'Gilliam',
    '41023': 'Grant',
    '41025': 'Harney',
    '41027': 'Hood River',
    '41029': 'Jackson',
    '41031': 'Jefferson',
    '41033': 'Josephine',
    '41035': 'Klamath',
    '41037': 'Lake',
    '41039': 'Lane',
    '41041': 'Lincoln',
    '41043': 'Linn',
    '41045': 'Malheur',
    '41047': 'Marion',
    '41049': 'Morrow',
    '41051': 'Multnomah',
    '41053': 'Polk',
    '41055': 'Sherman',
    '41057': 'Tillamook',
    '41059': 'Umatilla',
    '41061': 'Union',
    '41063': 'Wallowa',
    '41065': 'Wasco',
    '41067': 'Washington',
    '41069': 'Wheeler',
    '41071': 'Yamhill',
    # =========================================================================
    # IDAHO (16) - 44 counties
    # =========================================================================
    '16001': 'Ada',
    '16003': 'Adams',
    '16005': 'Bannock',
    '16007': 'Bear Lake',
    '16009': 'Benewah',
    '16011': 'Bingham',
    '16013': 'Blaine',
    '16015': 'Boise',
    '16017': 'Bonner',
    '16019': 'Bonneville',
    '16021': 'Boundary',
    '16023': 'Butte',
    '16025': 'Camas',
    '16027': 'Canyon',
    '16029': 'Caribou',
    '16031': 'Cassia',
    '16033': 'Clark',
    '16035': 'Clearwater',
    '16037': 'Custer',
    '16039': 'Elmore',
    '16041': 'Franklin',
    '16043': 'Fremont',
    '16045': 'Gem',
    '16047': 'Gooding',
    '16049': 'Idaho',
    '16051': 'Jefferson',
    '16053': 'Jerome',
    '16055': 'Kootenai',
    '16057': 'Latah',
    '16059': 'Lemhi',
    '16061': 'Lewis',
    '16063': 'Lincoln',
    '16065': 'Madison',
    '16067': 'Minidoka',
    '16069': 'Nez Perce',
    '16071': 'Oneida',
    '16073': 'Owyhee',
    '16075': 'Payette',
    '16077': 'Power',
    '16079': 'Shoshone',
    '16081': 'Teton',
    '16083': 'Twin Falls',
    '16085': 'Valley',
    '16087': 'Washington',
    # =========================================================================
    # MONTANA (30) - 56 counties
    # =========================================================================
    '30001': 'Beaverhead',
    '30003': 'Big Horn',
    '30005': 'Blaine',
    '30007': 'Broadwater',
    '30009': 'Carbon',
    '30011': 'Carter',
    '30013': 'Cascade',
    '30015': 'Chouteau',
    '30017': 'Custer',
    '30019': 'Daniels',
    '30021': 'Dawson',
    '30023': 'Deer Lodge',
    '30025': 'Fallon',
    '30027': 'Fergus',
    '30029': 'Flathead',
    '30031': 'Gallatin',
    '30033': 'Garfield',
    '30035': 'Glacier',
    '30037': 'Golden Valley',
    '30039': 'Granite',
    '30041': 'Hill',
    '30043': 'Jefferson',
    '30045': 'Judith Basin',
    '30047': 'Lake',
    '30049': 'Lewis and Clark',
    '30051': 'Liberty',
    '30053': 'Lincoln',
    '30055': 'McCone',
    '30057': 'Madison',
    '30059': 'Meagher',
    '30061': 'Mineral',
    '30063': 'Missoula',
    '30065': 'Musselshell',
    '30067': 'Park',
    '30069': 'Petroleum',
    '30071': 'Phillips',
    '30073': 'Pondera',
    '30075': 'Powder River',
    '30077': 'Powell',
    '30079': 'Prairie',
    '30081': 'Ravalli',
    '30083': 'Richland',
    '30085': 'Roosevelt',
    '30087': 'Rosebud',
    '30089': 'Sanders',
    '30091': 'Sheridan',
    '30093': 'Silver Bow',
    '30095': 'Stillwater',
    '30097': 'Sweet Grass',
    '30099': 'Teton',
    '30101': 'Toole',
    '30103': 'Treasure',
    '30105': 'Valley',
    '30107': 'Wheatland',
    '30109': 'Wibaux',
    '30111': 'Yellowstone',
}

# ---- SHARED POLYGON CACHE ----
# Multiple providers may share the same block groups, so cache polygons
_polygon_cache = {}


def find_csv_files_for_provider(provider_key):
    """Use glob to find CSV files matching the provider's state/ID pattern."""
    prov = PROVIDERS[provider_key]
    data_dir = os.path.dirname(os.path.abspath(__file__))
    provider_id = prov['id']
    found_files = {}

    for state_fips in prov['states']:
        pattern = os.path.join(data_dir, f"bdc_{state_fips}_{provider_id}_*.csv")
        matches = glob.glob(pattern)
        if matches:
            # If multiple matches, use the most recently modified one
            matches.sort(key=os.path.getmtime, reverse=True)
            found_files[state_fips] = matches[0]

    return found_files


def find_all_csv_files():
    """Find ALL BDC CSV files in the fcc_data/ directory for --all mode."""
    data_dir = os.path.dirname(os.path.abspath(__file__))
    pattern = os.path.join(data_dir, "bdc_*_*_*.csv")
    all_files = glob.glob(pattern)

    # Group by provider ID extracted from filename: bdc_{state}_{providerid}_...csv
    providers_found = defaultdict(dict)
    for fpath in all_files:
        fname = os.path.basename(fpath)
        parts = fname.split('_')
        if len(parts) >= 3:
            state_fips = parts[1]
            provider_id = parts[2]
            if state_fips in STATE_FIPS:
                providers_found[provider_id][state_fips] = fpath

    return providers_found


def process_csv_files(provider_key=None, csv_files=None, provider_id=None, tech_filter=50):
    """Read CSVs for a provider and aggregate by block group (fiber only).

    Can be called with a named provider_key (uses PROVIDERS config),
    or directly with csv_files dict and provider_id (for --all mode).
    """
    if provider_key:
        prov = PROVIDERS[provider_key]
        csv_files = find_csv_files_for_provider(provider_key)
        tech_filter = prov['tech_filter']
    elif csv_files is None:
        raise ValueError("Must provide either provider_key or csv_files")

    data_dir = os.path.dirname(os.path.abspath(__file__))

    block_groups = defaultdict(lambda: {
        'bsls': 0, 'blocks': set(), 'res_bsls': 0, 'bus_bsls': 0,
    })
    state_totals = defaultdict(lambda: {'bsls': 0, 'block_groups': set(), 'counties': set()})
    total_bsls = 0
    skipped = 0

    for state_fips, csv_path in sorted(csv_files.items()):
        if not os.path.exists(csv_path):
            print(f"  SKIP: {csv_path} not found")
            continue

        state_abbr = STATE_FIPS.get(state_fips, state_fips)
        count = 0
        state_skip = 0

        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                tech = int(row.get('technology', 0))
                if tech != tech_filter:
                    state_skip += 1
                    continue

                block_geoid = row['block_geoid']
                bg_id = block_geoid[:12]
                county_fips = block_geoid[:5]
                biz_res = row.get('business_residential_code', 'R')

                block_groups[bg_id]['bsls'] += 1
                block_groups[bg_id]['blocks'].add(block_geoid)
                if biz_res == 'R':
                    block_groups[bg_id]['res_bsls'] += 1
                else:
                    block_groups[bg_id]['bus_bsls'] += 1

                state_totals[state_abbr]['bsls'] += 1
                state_totals[state_abbr]['block_groups'].add(bg_id)
                state_totals[state_abbr]['counties'].add(county_fips)
                count += 1

        total_bsls += count
        skipped += state_skip
        print(f"  {state_abbr}: {count:,} fiber BSLs ({state_skip:,} non-fiber skipped), "
              f"{len(state_totals[state_abbr]['block_groups']):,} BGs, "
              f"{len(state_totals[state_abbr]['counties'])} counties")

    print(f"\n  TOTAL: {total_bsls:,} fiber BSLs across {len(block_groups):,} block groups "
          f"({skipped:,} non-fiber rows skipped)")

    result = {}
    for bg_id, data in block_groups.items():
        state_fips = bg_id[:2]
        county_fips = bg_id[:5]
        result[bg_id] = {
            'bsls': data['bsls'],
            'blocks': len(data['blocks']),
            'res': data['res_bsls'],
            'bus': data['bus_bsls'],
            'state': STATE_FIPS.get(state_fips, state_fips),
            'county': COUNTY_NAMES.get(county_fips, county_fips),
            'countyFips': county_fips,
            'tractId': bg_id[:11],
        }

    return result, state_totals


def fetch_block_group_polygons(block_groups_data):
    """Fetch block group polygons from TIGERweb, using cache for previously fetched."""
    global _polygon_cache

    base_url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer/8/query"

    # Filter to only block groups we don't already have cached
    needed = {bg_id for bg_id in block_groups_data if bg_id not in _polygon_cache}
    cached_count = len(block_groups_data) - len(needed)

    if cached_count > 0:
        print(f"  {cached_count} block groups already cached, {len(needed)} new to fetch")

    if not needed:
        return {bg_id: _polygon_cache[bg_id] for bg_id in block_groups_data}, []

    counties = defaultdict(set)
    for bg_id in needed:
        counties[bg_id[:5]].add(bg_id)

    print(f"  Fetching polygons for {len(needed)} block groups across {len(counties)} counties...")

    polygons = {}
    failed_counties = []

    for i, (county_fips, bg_ids) in enumerate(sorted(counties.items())):
        state_fips = county_fips[:2]
        county_code = county_fips[2:]
        county_name = COUNTY_NAMES.get(county_fips, county_fips)
        state_abbr = STATE_FIPS.get(state_fips, state_fips)

        params = urllib.parse.urlencode({
            'where': f"STATE='{state_fips}' AND COUNTY='{county_code}'",
            'outFields': 'GEOID,AREALAND,AREAWATER,HU100,POP100',
            'outSR': '4326',
            'f': 'geojson',
            'returnGeometry': 'true',
        }).encode()

        try:
            req = urllib.request.Request(
                base_url, data=params,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Accept': 'application/json',
                }
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            if 'features' in data:
                matched = 0
                for feature in data['features']:
                    geoid = feature['properties']['GEOID']
                    if geoid in bg_ids:
                        poly_data = {
                            'geometry': feature['geometry'],
                            'areaLand': feature['properties'].get('AREALAND', 0),
                            'areaWater': feature['properties'].get('AREAWATER', 0),
                            'hu100': feature['properties'].get('HU100', 0),
                            'pop100': feature['properties'].get('POP100', 0),
                        }
                        polygons[geoid] = poly_data
                        _polygon_cache[geoid] = poly_data
                        matched += 1
                status = "OK" if matched == len(bg_ids) else f"PARTIAL ({matched}/{len(bg_ids)})"
                print(f"  [{i+1}/{len(counties)}] {state_abbr} {county_name}: {matched} matched {status}")
            else:
                error = data.get('error', {}).get('message', 'Unknown error')
                print(f"  [{i+1}/{len(counties)}] {state_abbr} {county_name}: ERROR - {error}")
                failed_counties.append(county_fips)
        except Exception as e:
            print(f"  [{i+1}/{len(counties)}] {state_abbr} {county_name}: FAILED - {str(e)[:80]}")
            failed_counties.append(county_fips)

        if i > 0 and i % 10 == 0:
            time.sleep(0.3)

    # Merge cached + newly fetched
    all_polys = {}
    for bg_id in block_groups_data:
        if bg_id in _polygon_cache:
            all_polys[bg_id] = _polygon_cache[bg_id]
        elif bg_id in polygons:
            all_polys[bg_id] = polygons[bg_id]

    print(f"\n  Polygons: {len(all_polys)}/{len(block_groups_data)} "
          f"({len(all_polys)*100//max(len(block_groups_data),1)}%)")

    return all_polys, failed_counties


def simplify_coords(coords, tolerance=0.002):
    """Distance-based vertex decimation. tolerance=0.002 (~220m) is good for zoom 7-10 BG shading."""
    if len(coords) <= 4:
        return [[round(c[0], 4), round(c[1], 4)] for c in coords]
    simplified = [coords[0]]
    for i in range(1, len(coords)):
        dx = coords[i][0] - simplified[-1][0]
        dy = coords[i][1] - simplified[-1][1]
        if (dx * dx + dy * dy) > (tolerance * tolerance):
            simplified.append(coords[i])
    if simplified[-1] != coords[-1]:
        simplified.append(coords[-1])
    # 4 decimal places = ~11m precision, more than enough
    return [[round(c[0], 4), round(c[1], 4)] for c in simplified]


def generate_and_write(provider_key, block_groups_data, polygons,
                       display_name=None, provider_id=None, js_const=None,
                       js_file=None, brand=None, color=None):
    """Generate GeoJSON and write JS + stats files for a provider."""
    data_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(data_dir)

    # Use PROVIDERS config if available, otherwise use passed params
    if provider_key in PROVIDERS:
        prov = PROVIDERS[provider_key]
        display_name = display_name or prov['display']
        provider_id = provider_id or prov['id']
        js_const = js_const or prov['js_const']
        js_file = js_file or prov['js_file']
        brand = brand or prov['brand']
        color = color or prov['color']
    else:
        # For --all mode with unknown providers
        display_name = display_name or f"Provider {provider_id}"
        js_const = js_const or f"PROVIDER_{provider_id}_BDC_COVERAGE"
        js_file = js_file or f"provider_{provider_id}_bdc.js"
        brand = brand or display_name
        color = color or '#6B7280'

    features = []
    missing = 0

    for bg_id, bg_data in sorted(block_groups_data.items()):
        if bg_id not in polygons:
            missing += 1
            continue

        poly = polygons[bg_id]
        geom = poly['geometry']

        if geom['type'] == 'Polygon':
            geom = {'type': 'Polygon',
                     'coordinates': [simplify_coords(ring) for ring in geom['coordinates']]}
        elif geom['type'] == 'MultiPolygon':
            geom = {'type': 'MultiPolygon',
                     'coordinates': [[simplify_coords(ring) for ring in polygon]
                                     for polygon in geom['coordinates']]}

        hu100 = float(poly.get('hu100', 0) or 0)
        pop100 = float(poly.get('pop100', 0) or 0)
        area_land = float(poly.get('areaLand', 0) or 0)
        coverage_pct = round(bg_data['bsls'] / hu100 * 100, 1) if hu100 > 0 else 0

        features.append({
            'type': 'Feature',
            'properties': {
                'id': bg_id, 'bsls': bg_data['bsls'], 'blocks': bg_data['blocks'],
                'state': bg_data['state'], 'county': bg_data['county'],
                'areaLandSqKm': round(area_land / 1e6, 1),
                'hu100': int(hu100), 'pop100': int(pop100),
                'coveragePct': min(coverage_pct, 100),
            },
            'geometry': geom,
        })

    geojson = {'type': 'FeatureCollection', 'features': features}
    print(f"  {len(features)} block groups with polygons ({missing} missing)")

    # Count states
    states = set()
    for f in features:
        states.add(f['properties']['state'])

    # Write JS file
    js_path = os.path.join(parent_dir, js_file)
    js_content = f"""// {display_name} FCC BDC Coverage Data
// Source: FCC Broadband Data Collection, Jun 2025 filing
// Provider ID: {provider_id} | Brand: {brand}
// Technology: 50 (Fiber to the Premises) | Filtered to FTTP only
// Aggregated to census block group level
// Generated: {time.strftime('%Y-%m-%d %H:%M')}
// States: {', '.join(sorted(states))}
// Block Groups: {len(features):,} | Fiber BSLs: {sum(f['properties']['bsls'] for f in features):,}
//
// Data: https://broadbandmap.fcc.gov/data-download/fixed

const {js_const} = {json.dumps(geojson, separators=(',', ':'))};
"""
    with open(js_path, 'w') as f:
        f.write(js_content)
    file_size = os.path.getsize(js_path)
    print(f"  Written: {js_path} ({file_size / 1024 / 1024:.1f} MB)")

    # Write stats JSON
    stats_path = os.path.join(data_dir, f'{provider_key}_stats.json')
    state_stats = defaultdict(lambda: {'bsls': 0, 'blockGroups': 0, 'counties': set()})
    for bg_id, data in block_groups_data.items():
        st = data['state']
        state_stats[st]['bsls'] += data['bsls']
        state_stats[st]['blockGroups'] += 1
        state_stats[st]['counties'].add(data['county'])
    for st in state_stats:
        state_stats[st]['counties'] = sorted(list(state_stats[st]['counties']))

    stats = {
        'provider': display_name,
        'providerId': provider_id,
        'brand': brand,
        'filing': 'Jun 2025',
        'technology': 'Fiber to the Premises (50)',
        'totalFiberBSLs': sum(d['bsls'] for d in block_groups_data.values()),
        'totalBlockGroups': len(block_groups_data),
        'states': dict(state_stats),
    }
    with open(stats_path, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"  Stats: {stats_path}")

    return file_size


def process_single_provider(provider_key):
    """Full pipeline for a single configured provider."""
    prov = PROVIDERS[provider_key]

    print("\n" + "=" * 60)
    print(f"Processing: {prov['display']} (ID: {prov['id']})")
    print("=" * 60)

    # Find CSV files via glob
    csv_files = find_csv_files_for_provider(provider_key)
    if not csv_files:
        print(f"  No CSV files found for provider {prov['id']} in states {prov['states']}")
        print(f"  Expected pattern: bdc_{{state_fips}}_{prov['id']}_*.csv")
        return

    print(f"\n  Found {len(csv_files)} CSV file(s):")
    for sf, fp in sorted(csv_files.items()):
        print(f"    {STATE_FIPS.get(sf, sf)}: {os.path.basename(fp)}")

    print("\n[1/3] Processing CSV files (fiber only, tech=50)...")
    bg_data, state_totals = process_csv_files(provider_key=provider_key)

    if not bg_data:
        print("  No data found, skipping.")
        return

    print("\n[2/3] Fetching block group polygons from TIGERweb...")
    polygons, failed = fetch_block_group_polygons(bg_data)

    if failed:
        print(f"\n  Retrying {len(failed)} failed counties...")
        time.sleep(2)
        retry_bgs = {bg_id: d for bg_id, d in bg_data.items() if bg_id[:5] in failed}
        retry_polys, still_failed = fetch_block_group_polygons(retry_bgs)
        polygons.update(retry_polys)
        if still_failed:
            print(f"  Still failed: {', '.join(still_failed)}")

    print("\n[3/3] Generating output files...")
    generate_and_write(provider_key, bg_data, polygons)


def process_all_csvs():
    """Process ALL CSV files found in the fcc_data/ directory (--all mode).

    Groups files by provider ID, processes each provider even if not
    in the PROVIDERS config. Useful for consolidation candidate analysis.
    """
    print("\n" + "=" * 60)
    print("MODE: --all (processing every BDC CSV found)")
    print("=" * 60)

    providers_found = find_all_csv_files()
    if not providers_found:
        print("  No BDC CSV files found in fcc_data/ directory.")
        print("  Expected pattern: bdc_{state_fips}_{provider_id}_*.csv")
        return

    print(f"\n  Found {sum(len(v) for v in providers_found.values())} CSV file(s) "
          f"across {len(providers_found)} provider(s):")

    # Map provider IDs to known provider keys
    id_to_key = {str(prov['id']): key for key, prov in PROVIDERS.items()}

    for pid, files in sorted(providers_found.items()):
        known_key = id_to_key.get(pid, None)
        label = f" ({PROVIDERS[known_key]['display']})" if known_key else " (unknown)"
        states_str = ', '.join(STATE_FIPS.get(s, s) for s in sorted(files.keys()))
        print(f"    Provider {pid}{label}: {len(files)} state(s) [{states_str}]")

    for pid, csv_files in sorted(providers_found.items()):
        known_key = id_to_key.get(pid, None)

        if known_key:
            # Use configured provider
            process_single_provider(known_key)
        else:
            # Unknown provider -- process with generic settings
            provider_key = f"provider_{pid}"
            print("\n" + "=" * 60)
            print(f"Processing: Unknown Provider ID {pid}")
            print("=" * 60)

            print(f"\n  Found {len(csv_files)} CSV file(s):")
            for sf, fp in sorted(csv_files.items()):
                print(f"    {STATE_FIPS.get(sf, sf)}: {os.path.basename(fp)}")

            print("\n[1/3] Processing CSV files (fiber only, tech=50)...")
            bg_data, state_totals = process_csv_files(
                csv_files=csv_files, provider_id=pid, tech_filter=50
            )

            if not bg_data:
                print("  No data found, skipping.")
                continue

            print("\n[2/3] Fetching block group polygons from TIGERweb...")
            polygons, failed = fetch_block_group_polygons(bg_data)

            if failed:
                print(f"\n  Retrying {len(failed)} failed counties...")
                time.sleep(2)
                retry_bgs = {bg_id: d for bg_id, d in bg_data.items() if bg_id[:5] in failed}
                retry_polys, still_failed = fetch_block_group_polygons(retry_bgs)
                polygons.update(retry_polys)
                if still_failed:
                    print(f"  Still failed: {', '.join(still_failed)}")

            print("\n[3/3] Generating output files...")
            generate_and_write(
                provider_key, bg_data, polygons,
                display_name=f"Provider {pid}",
                provider_id=int(pid),
            )


if __name__ == '__main__':
    args = sys.argv[1:]

    if '--all' in args:
        process_all_csvs()
    elif args:
        # Process specific named providers
        for provider_key in args:
            if provider_key not in PROVIDERS:
                print(f"Unknown provider: {provider_key}. "
                      f"Available: {', '.join(PROVIDERS.keys())}")
                print(f"  Use --all to process every CSV file found.")
                continue
            process_single_provider(provider_key)
    else:
        # Default: process all configured providers
        for provider_key in PROVIDERS:
            process_single_provider(provider_key)

    print("\n" + "=" * 60)
    print("ALL DONE!")
    print("=" * 60)
