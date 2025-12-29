#!/usr/bin/env python3
"""
Test script to check Databento API access and estimate costs.
NO DATA IS FETCHED - only cost estimates.

Requires: pip install databento
Set env var: DATABENTO_API_KEY=your_key
"""
import os
import databento as db
from datetime import datetime, timedelta

# Make sure API key is set
api_key = os.getenv('DATABENTO_API_KEY')
if not api_key:
    print("ERROR: Set DATABENTO_API_KEY environment variable first")
    print("  export DATABENTO_API_KEY='your_key_here'")
    exit(1)

client = db.Historical(api_key)

print("=" * 60)
print("DATABENTO API TEST (NO DATA FETCHED)")
print("=" * 60)

# Check dataset access
print("\n1. CHECKING DATASET ACCESS")
print("-" * 40)
try:
    metadata = client.metadata.get_dataset_range(dataset='GLBX.MDP3')
    print(f"  ✅ GLBX.MDP3 (CME Group Futures)")
    print(f"     Available: {metadata.start_date} to {metadata.end_date}")
except Exception as e:
    print(f"  ❌ GLBX.MDP3: {str(e)[:60]}")
    print("\nCannot proceed without GLBX.MDP3 access.")
    exit(1)

# Test futures root symbols using 'parent' stype
print("\n2. FUTURES ROOTS CHECK (using 'parent' symbology)")
print("-" * 40)
print("Testing individual contract data (for volume-based roll detection)")

futures_roots = {
    'ES': 'E-mini S&P 500',
    'NQ': 'E-mini Nasdaq',
    'CL': 'Crude Oil',
    'NG': 'Natural Gas',
    'GC': 'Gold',
    'SI': 'Silver',
    'HG': 'Copper',
    'ZC': 'Corn',
    'ZS': 'Soybeans',
    'ZW': 'Wheat',
    'ZM': 'Soybean Meal',
    'ZL': 'Soybean Oil',
    'LE': 'Live Cattle',
    'HE': 'Lean Hogs',
    'GF': 'Feeder Cattle',
    '6E': 'Euro FX',
    '6J': 'Japanese Yen',
}

end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

available_roots = []
total_7day_cost = 0

for root, name in futures_roots.items():
    try:
        cost = client.metadata.get_cost(
            dataset='GLBX.MDP3',
            symbols=[root],
            stype_in='parent',  # Get all contracts for this root
            schema='ohlcv-1d',
            start=start_date,
            end=end_date,
        )
        print(f"  ✅ {root:4s} ({name}): ${cost:.4f} for 7 days")
        available_roots.append(root)
        total_7day_cost += cost
    except Exception as e:
        error_msg = str(e)
        if 'not found' in error_msg.lower() or 'invalid' in error_msg.lower():
            print(f"  ❌ {root:4s} ({name}): Not found")
        else:
            print(f"  ⚠️  {root:4s} ({name}): {error_msg[:40]}")

print(f"\n  7-day total for {len(available_roots)} roots: ${total_7day_cost:.2f}")

# Full year cost estimate
print("\n3. FULL YEAR COST ESTIMATE (2024)")
print("-" * 40)

if available_roots:
    try:
        total_cost = client.metadata.get_cost(
            dataset='GLBX.MDP3',
            symbols=available_roots,
            stype_in='parent',
            schema='ohlcv-1d',
            start='2024-01-01',
            end='2024-12-29',
        )
        print(f"  All {len(available_roots)} roots for full 2024: ${total_cost:.2f}")
    except Exception as e:
        print(f"  Error: {e}")
        # Try estimating by multiplying 7-day cost
        estimated = total_7day_cost * 52
        print(f"  Estimated (7-day × 52): ~${estimated:.2f}")

# Multi-year backfill estimate
print("\n4. BACKFILL COST ESTIMATE (2020-2024)")
print("-" * 40)

if available_roots:
    try:
        backfill_cost = client.metadata.get_cost(
            dataset='GLBX.MDP3',
            symbols=available_roots,
            stype_in='parent',
            schema='ohlcv-1d',
            start='2020-01-01',
            end='2024-12-29',
        )
        print(f"  5-year backfill ({len(available_roots)} roots): ${backfill_cost:.2f}")
    except Exception as e:
        print(f"  Error: {e}")

# Test stock symbols
print("\n5. STOCK SYMBOLS CHECK (XNAS.ITCH)")
print("-" * 40)

stock_samples = ['SPY', 'AAPL', 'MSFT', 'GOOGL', 'NVDA', 'TSLA', 'META', 'AMZN']
available_stocks = []
stock_7day_cost = 0

try:
    metadata = client.metadata.get_dataset_range(dataset='XNAS.ITCH')
    print(f"  ✅ XNAS.ITCH available: {metadata.start_date} to {metadata.end_date}")
    
    for symbol in stock_samples:
        try:
            cost = client.metadata.get_cost(
                dataset='XNAS.ITCH',
                symbols=[symbol],
                schema='ohlcv-1d',
                start=start_date,
                end=end_date,
            )
            print(f"  ✅ {symbol}: ${cost:.4f} for 7 days")
            available_stocks.append(symbol)
            stock_7day_cost += cost
        except Exception as e:
            print(f"  ❌ {symbol}: {str(e)[:40]}")
    
    print(f"\n  7-day total for {len(available_stocks)} stocks: ${stock_7day_cost:.4f}")
except Exception as e:
    print(f"  ❌ XNAS.ITCH not available: {str(e)[:50]}")

print("\n" + "=" * 60)
print("⚠️  NO DATA WAS FETCHED - only cost estimates above")
print("=" * 60)

print(f"""
SUMMARY
-------
Available futures roots: {available_roots}
Available stocks: {available_stocks}

Data Flow:
- FUTURES: databento_data.py → historical_data → build_continuous_prices.py → continuous_prices
- STOCKS:  databento_data.py → continuous_prices (direct, no roll needed)

Next Steps:
1. Run: python databento_data.py --dry-run   (test fetch, no DB write)
2. Run: python databento_data.py --start 2024-01-01  (backfill all)
3. Run: python databento_data.py --start 2024-01-01 --futures-only  (futures only)
4. Run: python databento_data.py --start 2024-01-01 --stocks-only   (stocks only)
5. Run: python ../seasonality_contprices_cj/build_continuous_prices.py  (roll futures)
""")
