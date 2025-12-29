#!/usr/bin/env python3
"""
Databento Seasonality Data Fetcher

Fetches daily OHLCV data from Databento for:
- Futures: Individual contracts → historical_data (for volume-based roll)
- Stocks: Direct prices → continuous_prices (no roll needed)

Usage:
    python databento_data.py                    # Fetch last 7 days
    python databento_data.py --start 2024-01-01 # Backfill from date
    python databento_data.py --start 2024-01-01 --end 2024-06-30
    python databento_data.py --dry-run          # Fetch but don't insert
    python databento_data.py --futures-only     # Skip stocks
    python databento_data.py --stocks-only      # Skip futures
    
Environment variables:
    DATABENTO_API_KEY - Your Databento API key
    DATABASE_URL - PostgreSQL connection string
"""
import os
import argparse
from datetime import datetime, date, timedelta
from urllib.parse import urlparse
import psycopg2
from psycopg2.extras import execute_values
import databento as db

# ------------ CONFIG ------------
SCHEMA_NAME = "seasonality"

# Futures root symbols to fetch
# Maps: Databento root -> DB symbol
FUTURES_ROOTS = {
    # Indices
    'ES': 'ES',   # E-mini S&P 500
    'NQ': 'NQ',   # E-mini Nasdaq
    # Energy  
    'CL': 'CL',   # Crude Oil
    'NG': 'NG',   # Natural Gas
    # Metals
    'GC': 'GC',   # Gold
    'SI': 'SI',   # Silver
    'HG': 'HG',   # Copper
    # Grains
    'ZC': 'ZC',   # Corn
    'ZS': 'ZS',   # Soybeans
    'ZW': 'ZW',   # Wheat
    'ZM': 'SM',   # Soybean Meal (DB uses SM)
    'ZL': 'BO',   # Soybean Oil (DB uses BO)
    # Meats
    'LE': 'LC',   # Live Cattle (DB uses LC)
    'HE': 'LH',   # Lean Hogs (DB uses LH)
    'GF': 'FC',   # Feeder Cattle
    # Currencies
    '6E': '6E',   # Euro FX
    '6J': '6J',   # Japanese Yen
}

# Databento datasets
FUTURES_DATASET = 'GLBX.MDP3'  # CME/CBOT/NYMEX/COMEX futures
STOCKS_DATASET = 'XNAS.ITCH'   # NASDAQ stocks

# Stock symbols to fetch (will be prefixed with STK in DB)
# These go directly to continuous_prices (no roll needed)
STOCK_SYMBOLS = [
    # Major indices ETFs
    'SPY', 'QQQ', 'IWM', 'DIA',
    # Mega caps
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B',
    # Tech
    'AMD', 'INTC', 'CRM', 'ORCL', 'ADBE', 'CSCO', 'AVGO', 'TXN',
    # Finance
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'V', 'MA', 'AXP',
    # Healthcare
    'UNH', 'JNJ', 'PFE', 'MRK', 'ABBV', 'LLY', 'TMO', 'ABT',
    # Consumer
    'WMT', 'HD', 'MCD', 'NKE', 'SBUX', 'TGT', 'COST', 'LOW',
    # Energy
    'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'MPC', 'PSX', 'VLO',
    # Industrials
    'CAT', 'DE', 'BA', 'GE', 'HON', 'UPS', 'RTX', 'LMT',
]

# Stock symbol prefix in database
STOCK_PREFIX = 'STK'


def get_db_connection():
    """Create database connection from DATABASE_URL."""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise RuntimeError('DATABASE_URL environment variable not set')
    
    url = urlparse(database_url)
    return psycopg2.connect(
        dbname=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port,
        sslmode='require',
    )


def get_databento_client():
    """Create Databento client."""
    api_key = os.getenv('DATABENTO_API_KEY')
    if not api_key:
        raise RuntimeError('DATABENTO_API_KEY environment variable not set')
    return db.Historical(api_key)


def get_active_contracts(root: str, trade_date: date) -> list:
    """
    Generate active contract symbols for a given root and date.
    Returns front 2-3 months of contracts.
    
    Contract months: F(Jan), G(Feb), H(Mar), J(Apr), K(May), M(Jun),
                    N(Jul), Q(Aug), U(Sep), V(Oct), X(Nov), Z(Dec)
    """
    month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
    contracts = []
    
    # Get next 3-4 contract months
    current_month = trade_date.month - 1  # 0-indexed
    current_year = trade_date.year % 100  # 2-digit year
    
    for i in range(4):
        month_idx = (current_month + i) % 12
        year = current_year if (current_month + i) < 12 else (current_year + 1) % 100
        contract = f"{root}{month_codes[month_idx]}{year:02d}"
        contracts.append(contract)
    
    return contracts


def fetch_futures_data(client, roots: list, start: str, end: str) -> list:
    """
    Fetch futures OHLCV data from Databento.
    Uses 'parent' symbol type to get all active contracts for each root.
    """
    if not roots:
        return []
    
    print(f"Fetching futures data for roots: {roots}")
    print(f"  Date range: {start} to {end}")
    
    # Use 'parent' stype to get all contracts for each root
    # Or we can request specific contracts
    all_records = []
    
    for root in roots:
        try:
            # Request all contracts for this root using parent symbology
            data = client.timeseries.get_range(
                dataset=FUTURES_DATASET,
                symbols=[root],
                stype_in='parent',  # Get all child contracts
                schema='ohlcv-1d',
                start=start,
                end=end,
            )
            df = data.to_df()
            if len(df) > 0:
                df['root'] = root
                records = df.reset_index().to_dict('records')
                all_records.extend(records)
                print(f"  {root}: {len(records)} records")
        except Exception as e:
            print(f"  {root}: Error - {str(e)[:60]}")
    
    print(f"  Total records: {len(all_records)}")
    return all_records


def insert_historical_data(conn, records: list, root_to_db: dict) -> int:
    """
    Insert price records into historical_data table.
    This table is used by build_continuous_prices to create the rolled series.
    """
    if not records:
        return 0
    
    rows = []
    for rec in records:
        root = rec.get('root', '')
        db_symbol = root_to_db.get(root)
        
        if not db_symbol:
            continue
        
        # Get contract symbol (e.g., ESH25)
        contract = rec.get('symbol', '')
        
        # Extract date from ts_event
        ts_event = rec.get('ts_event')
        if hasattr(ts_event, 'date'):
            trade_date = ts_event.date()
        elif isinstance(ts_event, str):
            trade_date = datetime.fromisoformat(ts_event.replace('Z', '+00:00')).date()
        else:
            # Nanoseconds timestamp
            trade_date = datetime.fromtimestamp(ts_event / 1e9).date()
        
        # Get OHLCV data
        open_px = float(rec.get('open', 0))
        high_px = float(rec.get('high', 0))
        low_px = float(rec.get('low', 0))
        close_px = float(rec.get('close', 0))
        volume = int(rec.get('volume', 0))
        
        # Skip invalid prices
        if close_px <= 0:
            continue
        
        rows.append((
            db_symbol,      # symbol
            trade_date,     # trade_date
            open_px,        # open
            high_px,        # high
            low_px,         # low
            close_px,       # close
            volume,         # value (volume for roll detection)
            contract,       # contract code
            None,           # instrument_id (optional)
        ))
    
    if not rows:
        return 0
    
    with conn.cursor() as cur:
        # Insert into historical_data
        # Uses contract_norm for conflict detection (computed column)
        execute_values(cur, f"""
            INSERT INTO {SCHEMA_NAME}.historical_data 
                (symbol, trade_date, open, high, low, close, value, contract, instrument_id)
            VALUES %s
            ON CONFLICT (symbol, trade_date, contract_norm) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                value = EXCLUDED.value
        """, rows, page_size=1000)
    
    conn.commit()
    return len(rows)


def ensure_assets_exist(conn, symbols: list, asset_type: str = "Futures"):
    """Ensure all symbols exist in assets table."""
    with conn.cursor() as cur:
        for symbol in symbols:
            cur.execute(f"""
                INSERT INTO {SCHEMA_NAME}.assets (symbol, name)
                VALUES (%s, %s)
                ON CONFLICT (symbol) DO NOTHING
            """, (symbol, f"{symbol} {asset_type}"))
    conn.commit()


def fetch_stock_data(client, symbols: list, start: str, end: str) -> list:
    """
    Fetch stock OHLCV data from Databento.
    """
    if not symbols:
        return []
    
    print(f"Fetching stock data for {len(symbols)} symbols")
    print(f"  Date range: {start} to {end}")
    
    all_records = []
    
    # Fetch in batches to avoid timeout
    batch_size = 50
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        try:
            data = client.timeseries.get_range(
                dataset=STOCKS_DATASET,
                symbols=batch,
                schema='ohlcv-1d',
                start=start,
                end=end,
            )
            df = data.to_df()
            if len(df) > 0:
                records = df.reset_index().to_dict('records')
                all_records.extend(records)
                print(f"  Batch {i//batch_size + 1}: {len(records)} records")
        except Exception as e:
            print(f"  Batch {i//batch_size + 1} error: {str(e)[:60]}")
    
    print(f"  Total stock records: {len(all_records)}")
    return all_records


def insert_stock_prices(conn, records: list) -> int:
    """
    Insert stock price records directly into continuous_prices table.
    Stocks don't need roll logic - they go straight to continuous_prices.
    """
    if not records:
        return 0
    
    rows = []
    for rec in records:
        # Get stock symbol and prefix with STK
        raw_symbol = rec.get('symbol', '')
        if not raw_symbol:
            continue
        db_symbol = f"{STOCK_PREFIX}{raw_symbol}"
        
        # Extract date from ts_event
        ts_event = rec.get('ts_event')
        if hasattr(ts_event, 'date'):
            trade_date = ts_event.date()
        elif isinstance(ts_event, str):
            trade_date = datetime.fromisoformat(ts_event.replace('Z', '+00:00')).date()
        else:
            trade_date = datetime.fromtimestamp(ts_event / 1e9).date()
        
        # Get OHLCV data
        open_px = float(rec.get('open', 0))
        high_px = float(rec.get('high', 0))
        low_px = float(rec.get('low', 0))
        close_px = float(rec.get('close', 0))
        
        # Skip invalid prices
        if close_px <= 0:
            continue
        
        rows.append((
            trade_date,
            db_symbol,
            open_px,
            high_px,
            low_px,
            close_px,
        ))
    
    if not rows:
        return 0
    
    with conn.cursor() as cur:
        # Insert directly into continuous_prices (stocks don't need roll)
        execute_values(cur, f"""
            INSERT INTO {SCHEMA_NAME}.continuous_prices 
                (trade_date, symbol, open, high, low, close)
            VALUES %s
            ON CONFLICT (trade_date, symbol) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close
        """, rows, page_size=1000)
    
    conn.commit()
    return len(rows)


def run(start_date: date | None, end_date: date | None, dry_run: bool = False,
        futures_only: bool = False, stocks_only: bool = False):
    """Main data fetching routine."""
    
    # Default to last 7 days if no dates specified
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=7)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    fetch_futures = not stocks_only
    fetch_stocks = not futures_only and len(STOCK_SYMBOLS) > 0
    
    print("=" * 60)
    print("DATABENTO SEASONALITY DATA FETCHER")
    print("=" * 60)
    print(f"Date range: {start_str} to {end_str}")
    print(f"Dry run: {dry_run}")
    print(f"Fetch futures: {fetch_futures} ({len(FUTURES_ROOTS)} symbols)")
    print(f"Fetch stocks: {fetch_stocks} ({len(STOCK_SYMBOLS)} symbols)")
    
    # Initialize clients
    client = get_databento_client()
    conn = get_db_connection()
    
    try:
        # ============ FUTURES ============
        if fetch_futures:
            print("\n" + "=" * 60)
            print("FUTURES → historical_data (for volume-based roll)")
            print("=" * 60)
            
            # Ensure futures assets exist
            db_symbols = list(set(FUTURES_ROOTS.values()))
            if not dry_run:
                ensure_assets_exist(conn, db_symbols, "Futures")
            
            # Fetch futures contract data
            records = fetch_futures_data(
                client, 
                list(FUTURES_ROOTS.keys()), 
                start_str, 
                end_str
            )
            
            if dry_run:
                print(f"\n[DRY RUN] Would insert {len(records)} futures records")
                if records:
                    print("Sample futures record:")
                    print(records[0])
            elif records:
                inserted = insert_historical_data(conn, records, FUTURES_ROOTS)
                print(f"\nInserted {inserted} records into historical_data")
        
        # ============ STOCKS ============
        if fetch_stocks:
            print("\n" + "=" * 60)
            print("STOCKS → continuous_prices (no roll needed)")
            print("=" * 60)
            
            # Ensure stock assets exist (with STK prefix)
            stock_db_symbols = [f"{STOCK_PREFIX}{s}" for s in STOCK_SYMBOLS]
            if not dry_run:
                ensure_assets_exist(conn, stock_db_symbols, "Stock")
            
            # Fetch stock data
            records = fetch_stock_data(client, STOCK_SYMBOLS, start_str, end_str)
            
            if dry_run:
                print(f"\n[DRY RUN] Would insert {len(records)} stock records")
                if records:
                    print("Sample stock record:")
                    print(records[0])
            elif records:
                inserted = insert_stock_prices(conn, records)
                print(f"\nInserted {inserted} records into continuous_prices")
        
        print("\n" + "=" * 60)
        print("COMPLETE")
        print("=" * 60)
        if fetch_futures:
            print("\nNext step: Run build_continuous_prices.py to roll futures contracts")
        
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch OHLCV data from Databento")
    parser.add_argument('--start', help='Start date YYYY-MM-DD (default: 7 days ago)')
    parser.add_argument('--end', help='End date YYYY-MM-DD (default: today)')
    parser.add_argument('--dry-run', action='store_true', help='Fetch but do not insert')
    parser.add_argument('--futures-only', action='store_true', help='Only fetch futures data')
    parser.add_argument('--stocks-only', action='store_true', help='Only fetch stock data')
    args = parser.parse_args()
    
    start_d = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else None
    end_d = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else None
    
    run(
        start_date=start_d, 
        end_date=end_d, 
        dry_run=args.dry_run,
        futures_only=args.futures_only,
        stocks_only=args.stocks_only,
    )
