# mrci_fill_staging_playwright.py
import re, time, argparse
from datetime import date, datetime, timedelta
from typing import Optional
import psycopg2
from psycopg2.extras import execute_batch, execute_values
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

# ------------ DB CONFIG ------------
db_url = urlparse(
    "postgresql://cotsui_db_user:dl2KHaaSiR0h7fQvmRdBNEsk9aukwJS6@"
    "dpg-d57mi72li9vc739j4ckg-a.oregon-postgres.render.com/cotsui_db_6605"
)
SCHEMA_NAME = "seasonality"

# ------------ SCRAPE CONFIG ------------
DEFAULT_START = date(2010, 1, 4)
THROTTLE_SECONDS = 0.4

# Use *exact* UA from your browser capture
BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
PERSIST_DIR = "./mrci_profile"   # stores cookies & cf_clearance between runs

def connect():
    conn = psycopg2.connect(
        dbname=db_url.path[1:],
        user=db_url.username,
        password=db_url.password,
        host=db_url.hostname,
        port=db_url.port,
        sslmode="require",
    )
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(f"SET search_path TO {SCHEMA_NAME};")
    return conn, cur

def ensure_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS mrci_contract_prices (
      id BIGSERIAL PRIMARY KEY,
      asset_id INT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
      trade_date DATE NOT NULL,
      open NUMERIC(18,6),
      high NUMERIC(18,6),
      low  NUMERIC(18,6),
      close NUMERIC(18,6),
      volume BIGINT,
      open_interest BIGINT,
      contract_code TEXT NOT NULL,
      UNIQUE(asset_id, trade_date, contract_code)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrape_log_mrci (
      id SMALLINT PRIMARY KEY DEFAULT 1,
      last_date DATE NOT NULL,
      updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)
    cur.execute("""
      INSERT INTO scrape_log_mrci (id, last_date)
      VALUES (1, %s)
      ON CONFLICT (id) DO NOTHING;
    """, (DEFAULT_START,))
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mrci_contract_base ON mrci_contract_prices (asset_id, trade_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mrci_contract_oi ON mrci_contract_prices (asset_id, trade_date, open_interest DESC);")

def get_last_scraped(cur, explicit_start: Optional[date]) -> date:
    if explicit_start:
        cur.execute("UPDATE scrape_log_mrci SET last_date=%s, updated_at=NOW() WHERE id=1;", (explicit_start,))
        return explicit_start
    cur.execute("SELECT last_date FROM scrape_log_mrci WHERE id=1;")
    row = cur.fetchone()
    return row[0] if row else DEFAULT_START

def update_last_scraped(cur, d: date):
    cur.execute("UPDATE scrape_log_mrci SET last_date=%s, updated_at=NOW() WHERE id=1;", (d,))

def load_asset_lookup(cur) -> dict[str, int]:
    cur.execute("SELECT id, symbol FROM assets;")
    return {symbol: asset_id for (asset_id, symbol) in cur.fetchall()}

def parse_html(html: str, d: date, asset_lookup: dict[str, int]) -> tuple[list[tuple], dict]:
    from bs4 import BeautifulSoup
    import re
    from datetime import datetime

    NAME_TO_ROOT = {
        "Soybeans(CBOT)": "S",
        "Soybean Meal(CBOT)": "SM",
        "Soybean Oil(CBOT)": "BO",
        "Corn(CBOT)": "C",
        "Wheat(CBOT)": "W",
        "Wheat(KCBT)": "KW",
        "Wheat(MGE)": "MW",
        "Oats(CBOT)": "O",
        "Rough Rice(CBOT)": "RR",
        "Live Cattle(CME)": "LC",
        "Feeder Cattle(CME)": "FC",
        "Lean Hogs(CME)": "LH",
        "Pork Bellies(CME)": "PB",
        "Class III Milk(CME)": "DA",
        'Cocoa(ICE)': "CC",
        'Coffee "C"(ICE)': "KC",
        "Sugar #11(ICE)": "SB",
        "Cotton(ICE)": "CT",
        "Orange Juice(ICE)": "OJ",
        "Canola(WCE)": "RS",
        "London Cocoa(LCE)": "LCC",
        "London Sugar(LCE)": "LSU",
    }

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="strat")

    rows: list[tuple] = []
    stats = {
        "had_pre": False,
        "had_table": bool(table),
        "lines_scanned": 0,
        "rows_parsed": 0,
        "rows_unknown_root": 0,
        "rows_bad_format": 0,
        "unknown_sections": set(),   # <-- new
    }

    if not table:
        return rows, stats

    def to_float(x: str):
        x = x.replace(",", "").strip()
        return None if x in ("", "-", "&nbsp;") else float(x)

    def to_int(x: str):
        x = x.replace(",", "").strip()
        return None if x in ("", "-", "&nbsp;") else int(x)

    def parse_yymmdd(x: str) -> date | None:
        x = x.strip()
        if not re.fullmatch(r"\d{6}", x):
            return None
        try:
            yy = int(x[:2])
            yyyy = 1900 + yy if yy >= 70 else 2000 + yy
            return datetime.strptime(str(yyyy) + x[2:], "%Y%m%d").date()
        except Exception:
            return None

    current_root: str | None = None

    for tr in table.find_all("tr"):
        th_note = tr.find("th", class_="note1")
        if th_note:
            name = " ".join(th_note.get_text(" ", strip=True).split())
            current_root = NAME_TO_ROOT.get(name)
            if not current_root:
                stats["unknown_sections"].add(name)
            continue

        tds = tr.find_all("td")
        if not tds:
            continue

        first_text = tds[0].get_text(strip=True).lower()
        if first_text.startswith("total volume"):
            continue

        if len(tds) < 8:
            continue

        stats["lines_scanned"] += 1

        if not current_root or current_root not in asset_lookup:
            stats["rows_unknown_root"] += 1
            continue

        mth = tds[0].get_text(strip=True)
        d_str = tds[1].get_text(strip=True)
        d_row = parse_yymmdd(d_str) or d

        try:
            o = to_float(tds[2].get_text())
            h = to_float(tds[3].get_text())
            l = to_float(tds[4].get_text())
            c = to_float(tds[5].get_text())
            vol = to_int(tds[7].get_text()) if len(tds) > 7 else None
            oi  = to_int(tds[8].get_text()) if len(tds) > 8 else None

            asset_id = asset_lookup[current_root]
            rows.append((asset_id, d_row, o, h, l, c, vol, oi, mth))
            stats["rows_parsed"] += 1
        except Exception:
            stats["rows_bad_format"] += 1
            continue

    # make set JSON-serializable if you ever log it
    stats["unknown_sections"] = sorted(stats["unknown_sections"])
    return rows, stats

def insert_rows(cur, batch: list[tuple]) -> int:
    if not batch:
        return 0
    sql = """
      INSERT INTO mrci_contract_prices
        (asset_id, trade_date, open, high, low, close, volume, open_interest, contract_code)
      VALUES %s
      ON CONFLICT (asset_id, trade_date, contract_code) DO NOTHING
      RETURNING 1;
    """
    # execute_values expands (%s) for all rows efficiently
    execute_values(cur, sql, batch, page_size=1000)
    # cursor.rowcount is reliable when we use RETURNING
    return cur.rowcount


def run(start: Optional[date], end: Optional[date]):
    end = end or date.today()
    conn, cur = connect()
    try:
        ensure_tables(cur)
        asset_lookup = load_asset_lookup(cur)
        if not asset_lookup:
            raise RuntimeError("assets table is empty — seed roots (CL, NG, ZS, …) first.")
        current = get_last_scraped(cur, start)
        conn.commit()

        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            # persistent context keeps cookies/cf_clearance across runs
            context = p.chromium.launch_persistent_context(
                user_data_dir=PERSIST_DIR,
                headless=True,  # set False once if you need to pass CF/consent
                viewport={"width": 1280, "height": 900},
                args=["--disable-blink-features=AutomationControlled"],
            )
            context.set_default_timeout(20000)
            # Set UA on all new requests
            context.set_extra_http_headers({"User-Agent": BROWSER_UA})

            page = context.new_page()
            # Warm the session on the yearly index (helps CF + Referer)
            year_url = f"https://www.mrci.com/ohlc/{current.year}/"
            try:
                page.goto(year_url, wait_until="domcontentloaded")
            except Exception:
                pass

            while current <= end:
                url = f"https://www.mrci.com/ohlc/{current.year}/{current.strftime('%y%m%d')}.php"
                print(f"Fetching {url}...")
                try:
                    page.goto(url, wait_until="domcontentloaded")
                    html = page.content()

                    if "<html" in html.lower():
                        batch, stats = parse_html(html, current, asset_lookup)
                        inserted = insert_rows(cur, batch)
                        update_last_scraped(cur, current)
                        conn.commit()

                        preview = batch[0] if batch else None
                        print(
                            f"  ✓ had_table={stats['had_table']} lines={stats['lines_scanned']} "
                            f"parsed={stats['rows_parsed']} unknown_root={stats['rows_unknown_root']} "
                            f"bad={stats['rows_bad_format']} inserted={inserted} "
                            f"unknown_sections={stats['unknown_sections']} "
                            f"preview={preview}"
                        )

                        if inserted == 0 and stats["rows_parsed"] == 0:
                            print("No Date for this day.")
                    else:
                        update_last_scraped(cur, current)
                        conn.commit()
                        print(f"  ⚠️ {current}: no data (blank page)")
                except Exception as e:
                    # still update checkpoint so you don't loop forever
                    update_last_scraped(cur, current)
                    conn.commit()
                    print(f"  ⚠️ {current}: error {e}")

                current += timedelta(days=1)
                time.sleep(THROTTLE_SECONDS)

            context.close()

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Fill mrci_contract_prices using Playwright (persistent browser)")
    ap.add_argument("--start", help="YYYY-MM-DD (default: resume checkpoint or 2010-01-04)")
    ap.add_argument("--end", help="YYYY-MM-DD (default: today)")
    args = ap.parse_args()

    start_d = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else None
    end_d = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else None

    run(start=start_d, end=end_d)
