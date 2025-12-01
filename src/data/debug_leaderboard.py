"""
debug_leaderboard.py
Script pour debugger l'extraction des leaderboards Strava
"""

import asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from bs4 import BeautifulSoup
from pathlib import Path


async def debug_segment(segment_id, save_html=True):
    """
    Debug l'extraction d'un segment spécifique
    
    Args:
        segment_id: ID du segment Strava
        save_html: Si True, sauvegarde le HTML pour inspection
    """
    segment_url = f"https://www.strava.com/segments/{segment_id}"
    
    print(f"{'='*70}")
    print(f"🔍 DEBUGGING SEGMENT {segment_id}")
    print(f"📍 URL: {segment_url}")
    print(f"{'='*70}\n")
    
    browser_config = BrowserConfig(
        headless=True,
        verbose=True
    )
    
    crawler_config = CrawlerRunConfig(
        wait_for="css:table tbody tr",
        page_timeout=20000,
        delay_before_return_html=3.0,
    )
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        print("⏳ Fetching page...\n")
        result = await crawler.arun(url=segment_url, config=crawler_config)
        
        # Save HTML
        if save_html:
            html_file = Path(f"debug_segment_{segment_id}.html")
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(result.html)
            print(f"💾 HTML saved to: {html_file.absolute()}\n")
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(result.html, 'html.parser')
        
        # Find ALL tables
        all_tables = soup.find_all('table')
        print(f"📊 Total tables found: {len(all_tables)}\n")
        
        if not all_tables:
            print("❌ No tables found in the page!")
            print("\n💡 Possible reasons:")
            print("   - Segment is private")
            print("   - No leaderboard data available")
            print("   - JavaScript not loaded properly")
            return
        
        # Analyze each table
        for idx, table in enumerate(all_tables, 1):
            print(f"\n{'─'*70}")
            print(f"📋 TABLE {idx}/{len(all_tables)}")
            print(f"{'─'*70}")
            
            # Table classes/attributes
            table_classes = table.get('class', [])
            table_id = table.get('id', 'No ID')
            print(f"   Classes: {table_classes}")
            print(f"   ID: {table_id}")
            
            # Find tbody
            tbody = table.find('tbody')
            if not tbody:
                print("   ⚠️  No <tbody> found")
                continue
            
            # Get rows
            rows = tbody.find_all('tr')
            print(f"   📏 Rows: {len(rows)}")
            
            if len(rows) == 0:
                print("   ⚠️  Empty table")
                continue
            
            # Analyze first row structure
            first_row = rows[0]
            cells = first_row.find_all('td')
            print(f"   📐 Cells per row: {len(cells)}")
            
            # Display cell contents
            print(f"\n   📝 First row contents:")
            for cell_idx, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                # Truncate long text
                display_text = text[:50] + "..." if len(text) > 50 else text
                print(f"      Cell {cell_idx}: '{display_text}'")
            
            # Look for time patterns
            print(f"\n   🕐 Looking for time data...")
            times_found = []
            for row_idx, row in enumerate(rows[:10], 1):  # Check first 10 rows
                cells = row.find_all('td')
                for cell_idx, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    # Check if looks like a time (contains : or ends with s)
                    if ':' in text or (text.endswith('s') and len(text) < 10):
                        times_found.append({
                            'row': row_idx,
                            'cell': cell_idx,
                            'value': text
                        })
            
            if times_found:
                print(f"   ✅ Found {len(times_found)} potential time values:")
                for t in times_found[:5]:  # Show first 5
                    print(f"      Row {t['row']}, Cell {t['cell']}: '{t['value']}'")
            else:
                print(f"   ❌ No time-like values found")
        
        # Summary
        print(f"\n{'='*70}")
        print(f"📊 SUMMARY")
        print(f"{'='*70}")
        print(f"Total tables: {len(all_tables)}")
        if times_found:
            print(f"✅ Time data found in table {idx}")
            print(f"   Most common cell index for times: {times_found[0]['cell']}")
        else:
            print(f"❌ No time data detected")


async def debug_multiple_segments(segment_ids):
    """Debug multiple segments"""
    print(f"\n🚀 Debugging {len(segment_ids)} segments...\n")
    
    for seg_id in segment_ids:
        await debug_segment(seg_id, save_html=False)
        print("\n" + "="*70 + "\n")
        await asyncio.sleep(1)  # Rate limiting


async def main():
    """Main debug function"""
    
    # Test cases
    print("Choose debug mode:")
    print("1. Debug single segment (saves HTML)")
    print("2. Debug failed segments from your log")
    print("3. Debug working vs non-working comparison")
    
    mode = input("\nEnter mode (1-3): ").strip()
    
    if mode == "1":
        segment_id = input("Enter segment ID: ").strip()
        await debug_segment(int(segment_id), save_html=True)
    
    elif mode == "2":
        # Segments that failed from your log
        failed_segments = [
            27052747,  # RN2 Ravine Glissante - Marocain
            19760378,  # piste skating barrière ulm
            23041395,  # 410m anneau descente
            21965446,  # parcours santé retour
            20442077,  # DH vers Cap Lahoussaye
            21967226,  # Parcours santé aller short
        ]
        await debug_multiple_segments(failed_segments)
    
    elif mode == "3":
        # Compare working vs non-working
        print("\n🔍 Comparing WORKING vs NON-WORKING segments\n")
        
        working = [28634019, 27009759]  # Ces segments ont marché
        non_working = [27052747, 19760378]  # Ces segments ont échoué
        
        print("="*70)
        print("✅ WORKING SEGMENTS")
        print("="*70)
        for seg_id in working:
            await debug_segment(seg_id, save_html=True)
            await asyncio.sleep(1)
        
        print("\n" + "="*70)
        print("❌ NON-WORKING SEGMENTS")
        print("="*70)
        for seg_id in non_working:
            await debug_segment(seg_id, save_html=True)
            await asyncio.sleep(1)
    
    else:
        print("Invalid mode")


if __name__ == "__main__":
    asyncio.run(main())