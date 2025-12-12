"""
Strava Leaderboard Extractor

Extracts leaderboard times from Strava segment pages using web scraping.
Supports two methods: AgentQL (with Playwright) or Crawl4AI (standalone).
"""

import asyncio
from bs4 import BeautifulSoup


class LeaderboardExtractor:
    """Extract leaderboard times from Strava segments"""
    
    def __init__(self, method="agentql", browser=None):
        """
        Initialize the extractor
        
        Args:
            method (str): Extraction method - "agentql" or "crawl4ai"
            browser: Playwright browser instance (required for AgentQL method)
        """
        self.method = method
        self.browser = browser
    
    @staticmethod
    def time_to_seconds(time_str):
        """
        Convert Strava time string to seconds
        
        Args:
            time_str (str): Time string from Strava (e.g., '24s', '5:24', '1:23:45')
        
        Returns:
            int: Time in seconds, or None if conversion fails
        """
        try:
            time_str = str(time_str).strip().lower()
            
            # Handle seconds format: "24s", "45seconds"
            if 's' in time_str:
                digits = ''.join(c for c in time_str if c.isdigit())
                return int(digits) if digits else None
            
            # Handle time format: "5:24" (MM:SS) or "1:23:45" (H:MM:SS)
            if ':' in time_str:
                parts = time_str.split(':')
                if len(parts) == 2:  # MM:SS
                    return int(parts[0]) * 60 + int(parts[1])
                elif len(parts) == 3:  # H:MM:SS
                    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            
            # Handle plain number
            if time_str.isdigit():
                return int(time_str)
            
            return None
            
        except (ValueError, AttributeError, TypeError):
            return None
    
    async def extract_times_agentql(self, segment_id):
        """
        Extract times using AgentQL (requires Playwright browser)
        
        Args:
            segment_id (int): Strava segment ID
        
        Returns:
            list: List of times in seconds (top 10)
        """
        if not self.browser:
            raise ValueError("Browser instance required for AgentQL method")
        
        import agentql
        
        segment_url = f"https://www.strava.com/segments/{segment_id}"
        
        try:
            # Open page with AgentQL wrapper
            page = await agentql.wrap_async(await self.browser.new_page())
            await page.goto(segment_url)
            await page.wait_for_timeout(1500)
            
            # Query the leaderboard table
            query = """
            {
              table {
                row[] {
                  time
                }
              }
            }
            """
            
            leaderboard_data = await page.query_data(query)
            await page.close()
            
            rows = leaderboard_data.get("table", {}).get("row", [])
            
            # Extract time strings
            times_str = [
                row.get("time") 
                for row in rows 
                if row.get("time") and str(row.get("time")).strip()
            ]
            
            # Convert to seconds
            times_seconds = []
            for t in times_str:
                seconds = self.time_to_seconds(t)
                if seconds is not None:
                    times_seconds.append(seconds)
            
            return times_seconds
                
        except Exception as e:
            print(f"  AgentQL error: {e}")
            return []
    
    async def extract_times_crawl4ai(self, segment_id):
        """
        Extract times using Crawl4AI (standalone, no browser required)
        
        Args:
            segment_id (int): Strava segment ID
        
        Returns:
            list: List of times in seconds (top 10)
        """
        try:
            from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
        except ImportError:
            print("⚠️ Crawl4AI not installed. Run: pip install crawl4ai")
            return []
        
        segment_url = f"https://www.strava.com/segments/{segment_id}"
        
        # Configure browser and crawler
        browser_config = BrowserConfig(
            headless=True,
            verbose=False
        )
        
        crawler_config = CrawlerRunConfig(
            wait_for="css:table tbody tr",
            page_timeout=20000,
            delay_before_return_html=3.0,
        )
        
        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=segment_url, config=crawler_config)
                
                # Parse HTML with BeautifulSoup
                soup = BeautifulSoup(result.html, 'html.parser')
                
                # Find the leaderboard table
                table = soup.find('table')
                if not table:
                    return []
                
                tbody = table.find('tbody')
                if not tbody:
                    return []
                
                # Extract top 10 rows
                rows = tbody.find_all('tr')[:10]
                
                times_seconds = []
                
                for row in rows:
                    cells = row.find_all('td')
                    
                    # Search for time cell (flexible approach)
                    time_found = None
                    for cell in cells:
                        text = cell.get_text(strip=True)
                        
                        # Identify time patterns:
                        # - Contains ':' but not '/' (e.g., 5:24 but not 5:24/km)
                        # - Ends with 's' and is short (e.g., 44s)
                        is_colon_time = ':' in text and '/' not in text and len(text) < 10
                        is_seconds_time = text.endswith('s') and len(text) < 6 and text[:-1].replace(':', '').isdigit()
                        
                        if is_colon_time or is_seconds_time:
                            seconds = self.time_to_seconds(text)
                            if seconds is not None:
                                time_found = seconds
                                break
                    
                    if time_found:
                        times_seconds.append(time_found)
                
                return times_seconds
                
        except Exception as e:
            print(f"  Crawl4AI error: {e}")
            return []
    
    async def get_times(self, segment_id):
        """
        Get leaderboard times for a segment
        
        Args:
            segment_id (int): Strava segment ID
        
        Returns:
            tuple: (best_time, average_top_10, tenth_best_time) in seconds
                   Returns (None, None, None) if extraction fails
        """
        # Extract times using selected method
        if self.method == "agentql":
            times_seconds = await self.extract_times_agentql(segment_id)
        elif self.method == "crawl4ai":
            times_seconds = await self.extract_times_crawl4ai(segment_id)
        else:
            raise ValueError(f"Unknown method: {self.method}")
        
        # Return None if no times extracted
        if not times_seconds:
            return None, None, None
        
        # Calculate metrics
        best_time = times_seconds[0]
        top_10 = times_seconds[:10]
        average_top_10 = sum(top_10) / len(top_10)
        tenth_best = times_seconds[9] if len(times_seconds) >= 10 else None
        
        return best_time, average_top_10, tenth_best


# Test function
async def test_extractor():
    """Test the extractor with a sample segment"""
    print("Testing LeaderboardExtractor with Crawl4AI...")
    
    extractor = LeaderboardExtractor(method="crawl4ai")
    
    # Test with a sample segment
    segment_id = 22495117
    best, avg, tenth = await extractor.get_times(segment_id)
    
    if best:
        print(f"✅ Segment {segment_id}:")
        print(f"   Best: {best}s")
        print(f"   Avg top 10: {avg:.1f}s")
        print(f"   10th: {tenth}s" if tenth else "   10th: N/A")
    else:
        print(f"❌ Failed to extract times")


if __name__ == "__main__":
    asyncio.run(test_extractor())