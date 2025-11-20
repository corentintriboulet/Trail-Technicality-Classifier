from pathlib import Path
import requests
import json
import time
import yaml
import os, sys
import csv
from datetime import datetime
import asyncio
import agentql
from agentql.ext.playwright.async_api import Page
from playwright.async_api import async_playwright


        
class StravaSegmentExtractor:
    def __init__(self, access_token):
        """
        Initialize with your Strava API access token
        Get token from: https://developers.strava.com/playground/#/
        """
        self.access_token = access_token
        self.base_url = "https://www.strava.com/api/v3"
        self.headers = {"Authorization": f"Bearer {access_token}"}
    
    def explore_segments(self, bounds, activity_type="riding", min_cat=None, max_cat=None):
        """
        Explore segments in a geographic area
        bounds: [sw_lat, sw_lng, ne_lat, ne_lng]
        activity_type: "riding" or "running"
        """
        url = f"{self.base_url}/segments/explore"
        params = {
            "bounds": ",".join(map(str, bounds)),
            "activity_type": activity_type
        }
        if min_cat:
            params["min_cat"] = min_cat
        if max_cat:
            params["max_cat"] = max_cat
        
        response = requests.get(url, headers=self.headers, params=params)
        if response.status_code == 200:
            return response.json().get("segments", [])
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return []
    
    def get_segment_details(self, segment_id):
        """Get detailed information about a segment"""
        url = f"{self.base_url}/segments/{segment_id}"
        response = requests.get(url, headers=self.headers)
        time.sleep(0.2)  # Rate limiting
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting segment {segment_id}: {response.status_code}")
            return None
    
    def get_segment_streams(self, segment_id):
        """Get altitude profile (elevation stream) for a segment"""
        url = f"{self.base_url}/segments/{segment_id}/streams"
        params = {
            "keys": "altitude,distance,latlng",
            "key_by_type": True
        }
        response = requests.get(url, headers=self.headers, params=params)
        time.sleep(0.2)  # Rate limiting
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting streams for segment {segment_id}")
            return None
    
    @staticmethod
    def time_to_seconds(time_str):
        """Convert time string (MM:SS or H:MM:SS) to seconds"""
        try:
            parts = time_str.strip().split(':')
            if len(parts) == 2:  # MM:SS
                return int(parts[0]) * 60 + int(parts[1])
            elif len(parts) == 3:  # H:MM:SS
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            else:
                return None
        except:
            return None
    
    async def scrape_leaderboard_times(self, segment_id):
        """Scrape leaderboard times using AgentQL"""
        segment_url = f"https://www.strava.com/segments/{segment_id}"
        
        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                page = await agentql.wrap_async(await browser.new_page())
                
                await page.goto(segment_url)
                await page.wait_for_timeout(2000)  # Wait for page to load
                
                # AgentQL query to extract leaderboard times
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
                await browser.close()
                
                # Extract times from response
                rows = leaderboard_data.get("table", {}).get("row", [])
                times_str = [row.get("time") for row in rows if row.get("time")]
                
                # Convert to seconds
                times_seconds = []
                for t in times_str:
                    seconds = self.time_to_seconds(t)
                    if seconds is not None:
                        times_seconds.append(seconds)
                
                if not times_seconds:
                    return None, None, None
                
                # Calculate metrics
                best_time = times_seconds[0] if len(times_seconds) > 0 else None
                top_10 = times_seconds[:10]
                average_top_10 = sum(top_10) / len(top_10) if top_10 else None
                tenth_best = times_seconds[9] if len(times_seconds) >= 10 else None
                
                return best_time, average_top_10, tenth_best
                
        except Exception as e:
            print(f"Error scraping leaderboard for segment {segment_id}: {e}")
            return None, None, None
    
    async def extract_segment_data_async(self, segment_id):
        """Extract all required data for a segment (async version with web scraping)"""
        # Get segment details from API
        details = self.get_segment_details(segment_id)
        if not details:
            return None
        
        # Get altitude profile from API
        streams = self.get_segment_streams(segment_id)
        
        # Scrape leaderboard times from web page
        print(f"  Scraping leaderboard for segment {segment_id}...")
        best_time, average_top_10, tenth_best = await self.scrape_leaderboard_times(segment_id)
        
        # Compile data
        segment_data = {
            "id": segment_id,
            "name": details.get("name"),
            "activity_type": details.get("activity_type"),
            "distance": details.get("distance"),  # in meters
            "elevation_gain": details.get("total_elevation_gain"),  # in meters
            "elevation_low": details.get("elevation_low"),
            "elevation_high": details.get("elevation_high"),
            "best_time": best_time,  # in seconds
            "average_top_10_time": average_top_10,  # in seconds
            "tenth_best_time": tenth_best,  # in seconds
            "total_effort_count": details.get("effort_count"),
            "total_athlete_count": details.get("athlete_count"),
            "altitude_profile": streams.get("altitude", {}).get("data", []) if streams else [],
            "distance_profile": streams.get("distance", {}).get("data", []) if streams else [],
            "coordinates": streams.get("latlng", {}).get("data", []) if streams else []
        }
        
        return segment_data
    
    def search_reunion_segments(self, max_segments=100):
        """
        Search for segments in Reunion Island
        Reunion Island coordinates: approximately -21.1 to -20.9 lat, 55.2 to 55.8 lng
        """
        # Define grid to cover Reunion Island
        lat_min, lat_max = -21.4, -20.8
        lng_min, lng_max = 55.2, 55.8
        
        all_segments = []
        segment_ids = set()
        
        # Grid search - divide area into smaller boxes
        grid_size = 4
        lat_step = (lat_max - lat_min) / grid_size
        lng_step = (lng_max - lng_min) / grid_size
        
        print("Searching for MTB/Gravel segments...")
        for i in range(grid_size):
            for j in range(grid_size):
                if len(segment_ids) >= max_segments:
                    break
                
                bounds = [
                    lat_min + i * lat_step,
                    lng_min + j * lng_step,
                    lat_min + (i + 1) * lat_step,
                    lng_min + (j + 1) * lng_step
                ]
                
                # Search for cycling segments (gravel/MTB)
                segments = self.explore_segments(bounds, activity_type="riding")
                for seg in segments:
                    if seg["id"] not in segment_ids and len(segment_ids) < max_segments:
                        segment_ids.add(seg["id"])
                        all_segments.append(seg)
                
                time.sleep(0.5)  # Rate limiting
        
        print(f"Searching for trail running segments...")
        segment_ids_running = set(segment_ids)
        
        for i in range(grid_size):
            for j in range(grid_size):
                if len(segment_ids_running) >= max_segments:
                    break
                
                bounds = [
                    lat_min + i * lat_step,
                    lng_min + j * lng_step,
                    lat_min + (i + 1) * lat_step,
                    lng_min + (j + 1) * lng_step
                ]
                
                # Search for running segments
                segments = self.explore_segments(bounds, activity_type="running")
                for seg in segments:
                    if seg["id"] not in segment_ids_running and len(all_segments) < max_segments:
                        segment_ids_running.add(seg["id"])
                        all_segments.append(seg)
                
                time.sleep(0.5)  # Rate limiting
        
        return all_segments[:max_segments]
    
    async def extract_all_data_async(self, max_segments=100):
        """Main function to extract all segment data (async version)"""
        print(f"Searching for up to {max_segments} segments in Reunion Island...")
        segments = self.search_reunion_segments(max_segments)
        
        print(f"Found {len(segments)} segments. Extracting detailed data...")
        detailed_data = []
        
        for i, seg in enumerate(segments, 1):
            print(f"Processing segment {i}/{len(segments)}: {seg.get('name')}")
            data = await self.extract_segment_data_async(seg["id"])
            if data:
                detailed_data.append(data)
        
        return detailed_data
    
    def save_to_json(self, data, filename="reunion_segments.json"):
        """Save data to JSON file"""
        project_root = Path(__file__).resolve().parents[2]
        raw_folder = project_root / "data" / "raw"
        os.makedirs(raw_folder, exist_ok=True)
        
        filepath = raw_folder / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to {filepath}")

    def save_to_csv(self, data, filename="reunion_segments.csv"):
        """Save data to CSV file (without altitude profile)"""
        if not data:
            print("No data to save")
            return
        
        project_root = Path(__file__).resolve().parents[2]
        raw_folder = project_root / "data" / "raw"
        os.makedirs(raw_folder, exist_ok=True)
        
        filepath = raw_folder / filename
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'id', 'name', 'activity_type', 'distance', 'elevation_gain',
                'elevation_low', 'elevation_high', 'best_time', 
                'average_top_10_time', 'tenth_best_time', 'total_effort_count', 
                'total_athlete_count'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for segment in data:
                row = {k: segment.get(k) for k in fieldnames}
                writer.writerow(row)
        
        print(f"CSV data saved to {filepath}")

    
    def naming(self):
        """Return the name for the saved files. Checking if already exists."""
        project_root = Path(__file__).resolve().parents[2]  # remonte jusqu'à 'Trail-Difficulty-Classifier'
        raw_folder = project_root / "data" / "raw"

        os.makedirs(raw_folder, exist_ok=True)  # assure que le dossier existe
        existing_files = os.listdir(raw_folder)

        version = 0
        while f"reunion_segments_{version}.json" in existing_files or f"reunion_segments_{version}.csv" in existing_files:
            version += 1
        json_name = f"reunion_segments_{version}.json"
        csv_name = f"reunion_segments_{version}.csv"
        return json_name, csv_name


# USAGE EXAMPLE
async def main():
    """Main async function"""
    
    # Load config from YAML file
    project_root = Path(__file__).resolve().parents[2]

    config_path = project_root / "config.yaml"

    with open(config_path, "r") as f:
         config = yaml.safe_load(f)
    
    ACCESS_TOKEN = config['strava']['access_token']
    AGENTQL_KEY = config['agentql']['api_key']

    
    # Initialize extractors
    extractor = StravaSegmentExtractor(ACCESS_TOKEN)
    os.environ["AGENTQL_API_KEY"] = config["agentql"]["api_key"]

    # Extract data for up to 100 segments
    data = await extractor.extract_all_data_async(max_segments=5)
    
    # Save results
    json_name, csv_name = extractor.naming()
    extractor.save_to_json(data, json_name)
    extractor.save_to_csv(data, csv_name)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Total segments extracted: {len(data)}")

if __name__ == "__main__":
    asyncio.run(main())