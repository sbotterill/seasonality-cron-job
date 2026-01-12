#!/usr/bin/env python3
"""
Debug script to check what Databento datasets and schemas are available for stocks.
"""
import os
import databento as db
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv('DATABENTO_API_KEY')
if not api_key:
    print("ERROR: DATABENTO_API_KEY not set")
    exit(1)

client = db.Historical(api_key)

print("=" * 60)
print("DATABENTO STOCK DATA DEBUG")
print("=" * 60)

# Test different datasets for stock data
STOCK_DATASETS = [
    'XNAS.ITCH',      # NASDAQ tick data
    'DBEQ.BASIC',     # Databento equities (daily bars)
    'XNYS.PILLAR',    # NYSE tick data
    'XNYS.TRADES',    # NYSE trades
]

SCHEMAS = ['ohlcv-1d', 'trades', 'tbbo', 'mbo']

test_symbols = ['SPY', 'AAPL', 'MSFT']
start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
end_date = datetime.now().strftime('%Y-%m-%d')

print(f"\nTest date range: {start_date} to {end_date}")
print(f"Test symbols: {test_symbols}")

# Check each dataset
for dataset in STOCK_DATASETS:
    print(f"\n{'='*60}")
    print(f"DATASET: {dataset}")
    print(f"{'='*60}")
    
    # Check if dataset is accessible
    try:
        metadata = client.metadata.get_dataset_range(dataset=dataset)
        print(f"✅ Available: {metadata.start_date} to {metadata.end_date}")
    except Exception as e:
        print(f"❌ Not accessible: {str(e)[:60]}")
        continue
    
    # Check available schemas
    print(f"\nTesting schemas for {test_symbols[0]}:")
    for schema in SCHEMAS:
        try:
            cost = client.metadata.get_cost(
                dataset=dataset,
                symbols=[test_symbols[0]],
                schema=schema,
                start=start_date,
                end=end_date,
            )
            print(f"  ✅ {schema}: ${cost:.4f}")
        except Exception as e:
            error_msg = str(e)
            if 'not_fully_available' in error_msg or 'schema' in error_msg.lower():
                print(f"  ❌ {schema}: Not available")
            else:
                print(f"  ⚠️  {schema}: {error_msg[:50]}")

# Try to find what actually works
print(f"\n{'='*60}")
print("FINDING WORKING COMBINATION")
print(f"{'='*60}")

working_combos = []
for dataset in ['DBEQ.BASIC', 'XNAS.ITCH']:
    for schema in ['ohlcv-1d', 'trades']:
        try:
            cost = client.metadata.get_cost(
                dataset=dataset,
                symbols=test_symbols,
                schema=schema,
                start=start_date,
                end=end_date,
            )
            print(f"✅ {dataset} + {schema}: ${cost:.4f} for 3 symbols, 30 days")
            working_combos.append((dataset, schema))
            
            # Try to actually fetch a small sample
            print(f"   Fetching sample data...")
            try:
                data = client.timeseries.get_range(
                    dataset=dataset,
                    symbols=['AAPL'],
                    schema=schema,
                    start=start_date,
                    end=start_date,  # Just 1 day
                )
                df = data.to_df()
                print(f"   ✅ Got {len(df)} rows")
                if len(df) > 0:
                    print(f"   Columns: {list(df.columns)}")
            except Exception as e2:
                print(f"   ⚠️  Fetch error: {str(e2)[:60]}")
                
        except Exception as e:
            pass  # Skip silently if not working

if working_combos:
    print(f"\n{'='*60}")
    print("RECOMMENDATION")
    print(f"{'='*60}")
    print(f"Use: {working_combos[0][0]} with schema={working_combos[0][1]}")
else:
    print("\n⚠️  No working combination found!")
    print("Check your Databento subscription includes equity data.")

