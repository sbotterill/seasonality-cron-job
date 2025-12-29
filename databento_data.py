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
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ------------ CONFIG ------------
SCHEMA_NAME = "seasonality"

# Futures root symbols to fetch
# Maps: Databento root -> DB symbol
FUTURES_ROOTS = {
    # ========== INDICES ==========
    'ES': 'ES',   # E-mini S&P 500
    'NQ': 'NQ',   # E-mini Nasdaq
    'YM': 'YM',   # Mini Dow
    'RTY': 'RTY', # E-mini Russell 2000
    
    # ========== ENERGY ==========
    'CL': 'CL',   # Crude Oil WTI
    'NG': 'NG',   # Natural Gas
    'RB': 'RB',   # RBOB Gasoline
    'HO': 'HO',   # Heating Oil
    
    # ========== METALS ==========
    'GC': 'GC',   # Gold
    'SI': 'SI',   # Silver
    'HG': 'HG',   # Copper
    'PL': 'PL',   # Platinum
    'PA': 'PA',   # Palladium
    'ALI': 'ALI', # Aluminum
    
    # ========== GRAINS ==========
    'ZC': 'ZC',   # Corn
    'ZS': 'ZS',   # Soybeans
    'ZW': 'ZW',   # Wheat (Chicago)
    'KE': 'KE',   # KC Wheat
    'ZM': 'ZM',   # Soybean Meal
    'ZL': 'ZL',   # Soybean Oil
    'ZO': 'ZO',   # Oats
    'ZR': 'ZR',   # Rough Rice
    
    # ========== MEATS ==========
    'LE': 'LE',   # Live Cattle
    'HE': 'HE',   # Lean Hogs
    'GF': 'GF',   # Feeder Cattle
    
    # ========== CURRENCIES ==========
    '6E': '6E',   # Euro FX
    '6J': '6J',   # Japanese Yen
    '6A': '6A',   # Australian Dollar
    '6B': '6B',   # British Pound
    '6C': '6C',   # Canadian Dollar
    '6S': '6S',   # Swiss Franc
    '6N': '6N',   # New Zealand Dollar
    '6M': '6M',   # Mexican Peso
    
    # ========== TREASURIES / RATES ==========
    'ZB': 'ZB',   # 30-Year T-Bond
    'ZN': 'ZN',   # 10-Year T-Note
    'ZF': 'ZF',   # 5-Year T-Note
    'ZT': 'ZT',   # 2-Year T-Note
    'ZQ': 'ZQ',   # 30-Day Fed Funds
    # 'GE': 'GE',   # Eurodollar (DELISTED - replaced by SOFR)
    'SR3': 'SR3', # 3-Month SOFR (replaced Eurodollar)
    'SR1': 'SR1', # 1-Month SOFR
    
    # ========== DAIRY ==========
    'DC': 'DC',   # Class III Milk
    'GNF': 'GNF', # Non-Fat Dry Milk
    'CB': 'CB',   # Cash-Settled Butter
    'CSC': 'CSC', # Cash-Settled Cheese
    
    # ========== CRYPTO (CME) ==========
    'BTC': 'BTC', # Bitcoin
    'ETH': 'ETH', # Ethereum
    
    # ========== LUMBER ==========
    'LBR': 'LBR', # Lumber
}

# Databento datasets
CME_DATASET = 'GLBX.MDP3'      # CME/CBOT/NYMEX/COMEX futures
ICE_DATASET = 'IFEU.IMPACT'    # ICE Europe futures (softs, energy)
STOCKS_DATASET = 'XNAS.ITCH'   # NASDAQ stocks

# ICE Futures (softs and other ICE products)
# Maps: Databento symbol -> DB symbol
# NOTE: ICE data requires separate Databento subscription (IFEU.IMPACT)
# Uncomment below if you have access
ICE_FUTURES_ROOTS = {
    # ========== SOFTS (ICE) - DISABLED - requires IFEU.IMPACT access ==========
    # 'KC': 'KC',   # Coffee C
    # 'CC': 'CC',   # Cocoa
    # 'SB': 'SB',   # Sugar #11
    # 'CT': 'CT',   # Cotton #2
    # 'OJ': 'OJ',   # Orange Juice (FCOJ-A)
    # 'DX': 'DX',   # US Dollar Index
}

# Stock symbols to fetch (will be prefixed with STK in DB)
# These go directly to continuous_prices (no roll needed)
STOCK_SYMBOLS = [
    # Major indices ETFs
    'SPY', 'QQQ', 'IWM', 'DIA',
    # Mega caps
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRKB',
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


def fetch_futures_data(client, roots: list, start: str, end: str, dataset: str, dataset_name: str = "") -> list:
    """
    Fetch futures OHLCV data from Databento.
    Uses continuous front-month contracts (.c.0 suffix).
    """
    if not roots:
        return []
    
    print(f"Fetching {dataset_name} futures for roots: {roots}")
    print(f"  Dataset: {dataset}")
    print(f"  Date range: {start} to {end}")
    
    all_records = []
    
    # Build continuous contract symbols (e.g., ES.c.0 for front month)
    # Databento uses .c.0 for front month, .c.1 for second month, etc.
    continuous_symbols = [f"{root}.c.0" for root in roots]
    
    try:
        # Fetch all continuous contracts in one request
        data = client.timeseries.get_range(
            dataset=dataset,
            symbols=continuous_symbols,
            stype_in='continuous',
            schema='ohlcv-1d',
            start=start,
            end=end,
        )
        df = data.to_df()
        if len(df) > 0:
            # Extract root from symbol (ES.c.0 -> ES)
            df['root'] = df['symbol'].apply(lambda x: x.split('.')[0] if '.' in str(x) else x)
            records = df.reset_index().to_dict('records')
            all_records.extend(records)
            
            # Print per-symbol counts
            symbol_counts = df.groupby('root').size()
            for root, count in symbol_counts.items():
                print(f"  {root}: {count} records")
    except Exception as e:
        print(f"  Error fetching continuous: {str(e)[:80]}")
        
        # Fallback: try individual symbols with raw_symbol
        print("  Trying individual symbol fetch...")
        for root in roots:
            try:
                # Try with .FUT suffix
                data = client.timeseries.get_range(
                    dataset=dataset,
                    symbols=[f"{root}.FUT"],
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
            except Exception as e2:
                print(f"  {root}: Error - {str(e2)[:50]}")
    
    print(f"  Total {dataset_name} records: {len(all_records)}")
    return all_records


def insert_futures_prices(conn, records: list, root_to_db: dict) -> int:
    """
    Insert futures price records directly into continuous_prices table.
    Since we're using Databento's continuous contracts, they're already rolled.
    """
    if not records:
        return 0
    
    rows = []
    for rec in records:
        root = rec.get('root', '')
        db_symbol = root_to_db.get(root)
        
        if not db_symbol:
            continue
        
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
        # Insert directly into continuous_prices
        # Databento's continuous contracts are already rolled
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
    Only uses ohlcv-1d schema (daily bars).
    """
    if not symbols:
        return []
    
    print(f"Fetching stock data for {len(symbols)} symbols")
    print(f"  Date range: {start} to {end}")
    
    all_records = []
    
    # Fetch in batches to avoid timeout
    batch_size = 20
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
                print(f"  Batch {i//batch_size + 1}: {len(records)} OHLCV records")
        except Exception as e:
            error_msg = str(e)
            if 'not_fully_available' in error_msg or 'not available' in error_msg.lower():
                print(f"  Batch {i//batch_size + 1}: ohlcv-1d not available for these dates")
                print(f"  Note: Stock OHLCV may require different subscription or dataset")
                break
            else:
                print(f"  Batch {i//batch_size + 1} error: {error_msg[:60]}")
    
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
    
    total_futures = len(FUTURES_ROOTS) + len(ICE_FUTURES_ROOTS)
    
    print("=" * 60)
    print("DATABENTO SEASONALITY DATA FETCHER")
    print("=" * 60)
    print(f"Date range: {start_str} to {end_str}")
    print(f"Dry run: {dry_run}")
    print(f"Fetch futures: {fetch_futures} ({len(FUTURES_ROOTS)} CME + {len(ICE_FUTURES_ROOTS)} ICE = {total_futures} symbols)")
    print(f"Fetch stocks: {fetch_stocks} ({len(STOCK_SYMBOLS)} symbols)")
    
    # Initialize clients
    client = get_databento_client()
    conn = get_db_connection()
    
    try:
        # ============ FUTURES ============
        if fetch_futures:
            total_futures_inserted = 0
            
            # --- CME/CBOT/NYMEX/COMEX ---
            print("\n" + "=" * 60)
            print("CME FUTURES → continuous_prices")
            print("=" * 60)
            
            # Ensure CME futures assets exist
            cme_db_symbols = list(set(FUTURES_ROOTS.values()))
            if not dry_run:
                ensure_assets_exist(conn, cme_db_symbols, "Futures")
            
            # Fetch CME futures
            cme_records = fetch_futures_data(
                client, 
                list(FUTURES_ROOTS.keys()), 
                start_str, 
                end_str,
                dataset=CME_DATASET,
                dataset_name="CME"
            )
            
            if dry_run:
                print(f"\n[DRY RUN] Would insert {len(cme_records)} CME futures records")
            elif cme_records:
                inserted = insert_futures_prices(conn, cme_records, FUTURES_ROOTS)
                total_futures_inserted += inserted
                print(f"\nInserted {inserted} CME records into continuous_prices")
            
            # --- ICE (Softs) ---
            if ICE_FUTURES_ROOTS:
                print("\n" + "=" * 60)
                print("ICE FUTURES (Softs) → continuous_prices")
                print("=" * 60)
                
                # Ensure ICE futures assets exist
                ice_db_symbols = list(set(ICE_FUTURES_ROOTS.values()))
                if not dry_run:
                    ensure_assets_exist(conn, ice_db_symbols, "Futures")
                
                # Fetch ICE futures
                ice_records = fetch_futures_data(
                    client, 
                    list(ICE_FUTURES_ROOTS.keys()), 
                    start_str, 
                    end_str,
                    dataset=ICE_DATASET,
                    dataset_name="ICE"
                )
                
                if dry_run:
                    print(f"\n[DRY RUN] Would insert {len(ice_records)} ICE futures records")
                elif ice_records:
                    inserted = insert_futures_prices(conn, ice_records, ICE_FUTURES_ROOTS)
                    total_futures_inserted += inserted
                    print(f"\nInserted {inserted} ICE records into continuous_prices")
            
            if not dry_run:
                print(f"\n>>> Total futures inserted: {total_futures_inserted}")
        
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
        print("\nData inserted directly into continuous_prices table.")
        
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
