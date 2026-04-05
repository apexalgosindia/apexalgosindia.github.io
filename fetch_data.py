"""
Fetches Apex Algos P&L data from Google Sheets and writes data.json.
CUSTOM VERSION for your specific data format (DD-MM-YY dates, ₹ symbols)

To add / remove / hide a strategy, edit ONLY the STRATEGIES list below.
"""
import requests, json, csv, io, datetime, statistics, sys

SHEET_ID = '1Rgy-HH8bcY7guN7PuH6biiK-JVV-PaFOH5aDII8oVf8'

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY CONFIG — this is the single source of truth.
#
# Fields:
#   key     → identifier used in data.json  (e.g. 'nap' → nap_daily, nap_monthly)
#   label   → display name shown on the website
#   capital → deployed capital in ₹ (used for ROI / Sharpe calculations)
#   col     → 0-based column index in the sheet (A=0, B=1, C=2 … L=11, S=18 …)
#   sheet   → Google Sheet tab name to read from (default: 'DAILY P&L')
#   color   → hex color for charts ('' = auto-assign from palette)
#   visible → True  = shown by default on the public site
#             False = data is fetched & stored but hidden unless admin toggles on
#
# To add a strategy from a different tab, just set sheet='DAILY P&L HEDGED'
# (or any other tab name) — the script fetches each unique tab only once.
# ══════════════════════════════════════════════════════════════════════════════
STRATEGIES = [
    # ── Standard strategies (DAILY P&L tab) ────────────────────────────────
    # key       label           capital     col   sheet            color       visible
    {'key':'nap',   'label':'NAP',   'capital':250_000, 'col':1,  'sheet':'DAILY P&L', 'color':'#D4A840', 'visible':True},
    {'key':'sap',   'label':'SAP',   'capital':250_000, 'col':4,  'sheet':'DAILY P&L', 'color':'#3FE088', 'visible':True},
    {'key':'cap',   'label':'CAP',   'capital':500_000, 'col':11, 'sheet':'DAILY P&L', 'color':'#B06FE0', 'visible':True},
    {'key':'napv2', 'label':'NAPv2', 'capital':250_000, 'col':18, 'sheet':'DAILY P&L', 'color':'#E06F6F', 'visible':True},
    {'key':'napv3', 'label':'NAPv3', 'capital':250_000, 'col':21, 'sheet':'DAILY P&L', 'color':'#E0B06F', 'visible':True},
    {'key':'sapv2', 'label':'SAPv2', 'capital':250_000, 'col':24, 'sheet':'DAILY P&L', 'color':'#6FE0D4', 'visible':True},
    {'key':'sapv3', 'label':'SAPv3', 'capital':250_000, 'col':27, 'sheet':'DAILY P&L', 'color':'#6F9FE0', 'visible':True},

    # ── Hedged strategies (DAILY P&L HEDGED tab) ───────────────────────────
    # Add your hedged strategies below — set col to the correct column index.
    # Example (update col/label/capital/color/visible to match your sheet):
    # {'key':'naph',  'label':'NAP-H',  'capital':250_000, 'col':1,  'sheet':'DAILY P&L HEDGED', 'color':'#A0E06F', 'visible':True},
    # {'key':'saph',  'label':'SAP-H',  'capital':250_000, 'col':4,  'sheet':'DAILY P&L HEDGED', 'color':'#E06FB0', 'visible':True},
]
# ══════════════════════════════════════════════════════════════════════════════

_S = {s['key']: s for s in STRATEGIES}   # quick lookup by key


# ── Helpers ──────────────────────────────────────────────────────────────────

def clean_number(value):
    """Remove ₹, commas, quotes and convert to float."""
    if not value or not value.strip():
        return None
    cleaned = value.replace('₹', '').replace(',', '').replace('"', '').strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_date_ddmmyy(date_str):
    """Parse date in DD-MM-YY (Day of Week) format, e.g. '02-03-26 (Mon)'."""
    date_part = date_str.split('(')[0].strip()
    try:
        return datetime.datetime.strptime(date_part, '%d-%m-%y')
    except ValueError:
        return None


def fetch_csv(sheet_name):
    url = (
        f'https://docs.google.com/spreadsheets/d/{SHEET_ID}'
        f'/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(sheet_name)}'
    )
    print(f"  Fetching: {url[:80]}...")
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    print(f"  ✓ Response received: {len(r.text)} bytes")
    return r.text


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_sheet(raw_csv, sheet_strategies, sheet_name=''):
    """Parse one sheet tab for a given subset of strategies.

    Also scans for numeric columns not covered by any strategy in STRATEGIES
    and prints a warning so you know to add them to the config.
    """
    reader  = csv.reader(io.StringIO(raw_csv))
    rows    = list(reader)
    records = []

    print(f"  Found {len(rows)} total rows")

    # Auto-detect first data row
    start_row = 0
    for i, row in enumerate(rows):
        if row and row[0].strip() and parse_date_ddmmyy(row[0]):
            start_row = i
            print(f"  Auto-detected data start at row {i}: '{row[0]}'")
            break

    # ── Header row detection (row just before data, if it exists) ────────────
    header_row = rows[start_row - 1] if start_row > 0 else []

    for row in rows[start_row:]:
        if not row or not row[0].strip():
            continue
        date = parse_date_ddmmyy(row[0])
        if not date:
            continue
        entry     = {'date': date}
        any_value = False
        for s in sheet_strategies:
            val = clean_number(row[s['col']]) if len(row) > s['col'] else None
            entry[s['key']] = val if val is not None else 0
            if val is not None:
                any_value = True
        if any_value:
            records.append(entry)

    records.sort(key=lambda x: x['date'])
    dates = [r['date'].strftime('%d/%m/%y') for r in records]
    daily = {s['key']: [r[s['key']] for r in records] for s in sheet_strategies}

    print(f"  ✓ Parsed {len(records)} daily records")
    if records:
        print(f"  Date range: {dates[0]} to {dates[-1]}")

    # ── Warn about unconfigured numeric columns ───────────────────────────────
    configured_cols = {s['col'] for s in STRATEGIES if s.get('sheet', 'DAILY P&L') == sheet_name}
    unconfigured    = []
    if records:
        # Sample the first data row to find all numeric columns
        sample_row = rows[start_row]
        for col_idx, cell in enumerate(sample_row):
            if col_idx == 0:
                continue  # skip date column
            if clean_number(cell) is not None and col_idx not in configured_cols:
                header = header_row[col_idx].strip() if col_idx < len(header_row) else ''
                unconfigured.append((col_idx, header))

    if unconfigured:
        print(f"\n  ⚠️  UNCONFIGURED COLUMNS detected in '{sheet_name}':")
        print(f"     These columns have numeric data but are NOT in your STRATEGIES list.")
        print(f"     Add them to fetch_data.py if you want them tracked:\n")
        for col_idx, header in unconfigured:
            col_letter = _col_letter(col_idx)
            hint = f'  ← header: "{header}"' if header else ''
            print(f"     col={col_idx} ({col_letter}){hint}")
            print(f"     {{'key':'???', 'label':'???', 'capital':250_000, 'col':{col_idx}, 'sheet':'{sheet_name}', 'color':'', 'visible':True}},")
        print()

    return dates, daily


def _col_letter(n):
    """Convert 0-based column index to spreadsheet letter (0→A, 27→AB …)."""
    s = ''
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# Keep old name as alias
def parse_daily_pnl(raw_csv):
    sheet = 'DAILY P&L'
    return parse_sheet(raw_csv, [s for s in STRATEGIES if s.get('sheet', 'DAILY P&L') == sheet], sheet)


def build_monthly_from_daily(daily_dates, daily):
    """Aggregate daily dict → monthly dict, driven by STRATEGIES config."""
    if not daily_dates:
        return [], {s['key']: [] for s in STRATEGIES}

    monthly_data = {}
    for i, date_str in enumerate(daily_dates):
        d           = datetime.datetime.strptime(date_str, '%d/%m/%y')
        month_key   = d.strftime('%Y-%m')
        month_label = d.strftime('%b %y')
        if month_key not in monthly_data:
            monthly_data[month_key] = {'label': month_label,
                                       **{s['key']: 0 for s in STRATEGIES}}
        for s in STRATEGIES:
            monthly_data[month_key][s['key']] += daily[s['key']][i]

    sorted_keys = sorted(monthly_data.keys())
    months  = [monthly_data[m]['label'] for m in sorted_keys]
    monthly = {s['key']: [monthly_data[m][s['key']] for m in sorted_keys]
               for s in STRATEGIES}

    print(f"  ✓ Built {len(months)} monthly records from daily data")
    return months, monthly


# ── Stats helpers ─────────────────────────────────────────────────────────────

def sharpe(daily_list, capital):
    if len(daily_list) < 2:
        return 0
    r      = [d / capital for d in daily_list]
    mean_r = statistics.mean(r)
    std_r  = statistics.stdev(r)
    if std_r == 0:
        return 0
    return round((mean_r / std_r) * (252 ** 0.5), 2)


def max_drawdown(daily_list, capital):
    cum = peak = 0
    dd  = 0
    for d in daily_list:
        cum  += d
        peak  = max(peak, cum)
        dd    = min(dd, (cum - peak) / capital)
    return round(dd * 100, 1)


def avg_monthly_roi(monthly_pnl, capital):
    """Average monthly ROI using only complete, active months."""
    if not monthly_pnl:
        return 0
    complete = monthly_pnl[:-1]
    active   = [v for v in complete if v != 0]
    if not active:
        return 0
    return round((sum(active) / len(active) / capital) * 100, 1)


def win_rate(daily_list):
    if not daily_list:
        return 0
    return round(sum(1 for d in daily_list if d > 0) / len(daily_list) * 100, 1)


# ── Main builder ──────────────────────────────────────────────────────────────

def build_data():
    # ── 1. Group strategies by sheet tab, fetch each tab once ────────────────
    from collections import defaultdict
    sheet_groups = defaultdict(list)
    for s in STRATEGIES:
        sheet_groups[s.get('sheet', 'DAILY P&L')].append(s)

    all_dates_sets = {}   # sheet_name → set of date strings
    all_daily      = {}   # sheet_name → {key: [values]}

    for sheet_name, strats in sheet_groups.items():
        print(f"\nFetching sheet: '{sheet_name}' ({len(strats)} strategies)...")
        try:
            raw_csv    = fetch_csv(sheet_name)
            dates, daily = parse_sheet(raw_csv, strats, sheet_name)
            all_dates_sets[sheet_name] = dates
            all_daily[sheet_name]      = daily
        except Exception as e:
            print(f"✗ Error fetching '{sheet_name}': {e}")
            import traceback; traceback.print_exc()
            all_dates_sets[sheet_name] = []
            all_daily[sheet_name]      = {s['key']: [] for s in strats}

    # ── 2. Build a unified sorted date spine across all sheets ───────────────
    all_date_set = set()
    for dates in all_dates_sets.values():
        all_date_set.update(dates)

    if not all_date_set:
        print("\n⚠️  WARNING: No daily data found across any sheet!")
        sys.exit(1)

    unified_dates = sorted(all_date_set,
                           key=lambda d: datetime.datetime.strptime(d, '%d/%m/%y'))

    # ── 3. Map each strategy onto the unified spine (0 for missing dates) ────
    daily = {}
    for sheet_name, strats in sheet_groups.items():
        sheet_dates = all_dates_sets[sheet_name]
        date_to_idx = {d: i for i, d in enumerate(sheet_dates)}
        for s in strats:
            raw = all_daily[sheet_name].get(s['key'], [])
            daily[s['key']] = [
                raw[date_to_idx[d]] if d in date_to_idx else 0
                for d in unified_dates
            ]

    daily_dates = unified_dates

    # ── 4. Build monthly aggregates ──────────────────────────────────────────
    print("\nAggregating monthly data from daily records...")
    months, monthly = build_monthly_from_daily(daily_dates, daily)

    today_formatted = datetime.date.today().strftime('%d/%m/%y')

    # ── 5. Per-strategy stats ─────────────────────────────────────────────────
    stats = {}
    for s in STRATEGIES:
        k   = s['key']
        cap = s['capital']
        d   = daily[k]
        m   = monthly[k]
        stats[k] = {
            'roi':           round(sum(m) / cap * 100, 1) if m else 0,
            'avg_month_roi': avg_monthly_roi(m, cap),
            'pnl':           round(sum(m)) if m else 0,
            'sharpe':        sharpe(d, cap),
            'max_dd':        max_drawdown(d, cap),
            'win_rate':      win_rate(d),
            'days':          len([x for x in d if x != 0]),
        }

    # Legacy combined stat (NAP + SAP)
    nap_d = daily.get('nap', []);  sap_d = daily.get('sap', [])
    nap_m = monthly.get('nap', []); sap_m = monthly.get('sap', [])
    comb  = [n + s for n, s in zip(nap_d, sap_d)] if nap_d and sap_d else []
    nc = _S.get('nap', {}).get('capital', 250_000)
    sc = _S.get('sap', {}).get('capital', 250_000)
    stats['comb'] = {
        'roi':      round((sum(nap_m) + sum(sap_m)) / (nc + sc) * 100, 1)
                    if nap_m and sap_m else 0,
        'pnl':      round(sum(nap_m) + sum(sap_m)) if nap_m and sap_m else 0,
        'sharpe':   sharpe(comb, nc + sc),
        'max_dd':   max_drawdown(comb, nc + sc),
        'win_rate': win_rate(comb),
    }

    # ── 6. Assemble output ────────────────────────────────────────────────────
    data = {
        'updated':    today_formatted,
        'strategies': [
            {
                'key':     s['key'],
                'label':   s['label'],
                'capital': s['capital'],
                'color':   s['color'],
                'visible': s['visible'],
                'sheet':   s.get('sheet', 'DAILY P&L'),
            }
            for s in STRATEGIES
        ],
        'months':      months,
        'daily_dates': daily_dates,
        'stats':       stats,
    }

    # Flat arrays for backwards-compat
    for s in STRATEGIES:
        data[f"{s['key']}_monthly"] = monthly[s['key']]
        data[f"{s['key']}_daily"]   = daily[s['key']]

    # Print summary
    print(f"\n✓ Summary:")
    print(f"  - Monthly records : {len(months)}")
    print(f"  - Daily records   : {len(daily_dates)}")
    print(f"  - Date range      : {daily_dates[0]} to {daily_dates[-1]}")
    print(f"  - Updated         : {today_formatted}")
    for s in STRATEGIES:
        vis   = '(visible)' if s['visible'] else '(hidden) '
        sheet = s.get('sheet', 'DAILY P&L')
        tag   = '' if sheet == 'DAILY P&L' else f' [{sheet}]'
        print(f"  - {s['label']:8s} {vis}: ₹{sum(monthly[s['key']]):,.0f}{tag}")
    print(f"  - Combined ROI    : {data['stats']['comb']['roi']}%")

    return data


if __name__ == '__main__':
    try:
        data = build_data()
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=2)
        print(f"\n✓ data.json written successfully!")
    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(1)
