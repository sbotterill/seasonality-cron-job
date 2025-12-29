#!/usr/bin/env python3
"""
One-time script to pass Cloudflare challenge and save cookies.
Run this once, wait for the browser to load the actual MRCI page, 
then press Enter to save and close.
"""
from playwright.sync_api import sync_playwright
import time

PERSIST_DIR = "./mrci_profile"

def main():
    print("Opening browser to pass Cloudflare challenge...")
    print("Wait for the MRCI page to fully load (you should see commodity prices).")
    print("Once loaded, come back here and press Enter to save the session.\n")
    
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=PERSIST_DIR,
            headless=False,
            viewport={"width": 1280, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        
        page = context.new_page()
        
        # Go to a page that will have data
        url = "https://www.mrci.com/ohlc/2024/241220.php"
        print(f"Navigating to {url}")
        page.goto(url, wait_until="domcontentloaded")
        
        # Wait for user to confirm
        input("\n>>> Press ENTER once the page shows commodity data (not 'Just a moment...') <<<\n")
        
        # Verify we passed
        html = page.content()
        if "Just a moment" in html:
            print("❌ Still on Cloudflare page! Try waiting longer or solving captcha.")
        elif 'class="strat"' in html:
            print("✅ SUCCESS! Cloudflare passed and cookies saved.")
            print("You can now run mrci_data.py in headless mode.")
        else:
            print("⚠️  Page loaded but no data table found. Check the browser.")
        
        context.close()

if __name__ == "__main__":
    main()

