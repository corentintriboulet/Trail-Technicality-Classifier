"""
Strava Segment Data Extractor

Extracts segment data from Strava API including:
- Segment search in geographic area
- Leaderboard scraping (best time, top 10 average, 10th place)
- Segment details and altitude profiles
- Multi-account rate limit handling

Data is saved incrementally to both Parquet and CSV formats.
"""

from pathlib import Path
import requests
import time
import yaml
import os
import asyncio
import pandas as pd
from Strava_Token_Manager import StravaTokenManager, make_strava_request_with_retry, RateLimitException
from Strava_Leaderboard_Extractor import LeaderboardExtractor


def load_config(config_path):
    """Load YAML configuration file"""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class StravaSegmentExtractor:
    """Extract and process Strava segment data"""
    
    def __init__(self, token_manager):
        """
        Initialize the segment extractor
        
        Args:
            token_manager: StravaTokenManager instance for API authentication
        """
        self.token_manager = token_manager
        self.base_url = "https://www.strava.com/api/v3"
        
        # Setup project paths
        self.project_root = Path(__file__).resolve().parents[2]
        self.raw_folder = self.project_root / "data" / "raw"
        os.makedirs(self.raw_folder, exist_ok=True)
        
        # Leaderboard extractor (initialized when needed)
        self.leaderboard_extractor = None
    
    def _make_api_request(self, url, params=None):
        """
        Centralized API request handler with automatic retry and account switching
        
        Args:
            url (str): API endpoint URL
            params (dict): Query parameters
        
        Returns:
            Response object or None if request fails
        """
        return make_strava_request_with_retry(self.token_manager, url, params)
    
    def explore_segments(self, bounds, activity_type="riding"):
        """
        Get segments in a geographic area
        
        Args:
            bounds (list): [lat_min, lng_min, lat_max, lng_max]
            activity_type (str): "riding" or "running"
        
        Returns:
            list: List of segment basic data dictionaries
        """
        url = f"{self.base_url}/segments/explore"
        params = {
            "bounds": ",".join(map(str, bounds)),
            "activity_type": activity_type
        }
        
        response = self._make_api_request(url, params)
        
        if response and response.status_code == 200:
            return response.json().get("segments", [])
        return []
    
    def get_segment_streams(self, segment_id):
        """
        Get altitude and distance profile for a segment
        
        Args:
            segment_id (int): Strava segment ID
        
        Returns:
            dict: Stream data (altitude, distance, latlng) or None if request fails
        """
        url = f"{self.base_url}/segments/{segment_id}/streams"
        params = {"keys": "altitude,distance,latlng", "key_by_type": True}
        
        response = self._make_api_request(url, params)
        
        if response and response.status_code == 200:
            return response.json()
        return None
    
    def get_segment_details(self, segment_id):
        """
        Get detailed segment information
        
        Args:
            segment_id (int): Strava segment ID
        
        Returns:
            dict: Segment details or None if request fails
        """
        url = f"{self.base_url}/segments/{segment_id}"
        
        response = self._make_api_request(url)
        
        if response and response.status_code == 200:
            return response.json()
        return None
    
    async def init_browser(self):
        """Initialize LeaderboardExtractor with Crawl4AI (no Playwright needed)"""
        if not self.leaderboard_extractor:
            self.leaderboard_extractor = LeaderboardExtractor(
                method="crawl4ai",
                browser=None
            )
    
    async def close_browser(self):
        """Close LeaderboardExtractor resources"""
        self.leaderboard_extractor = None
    
    async def extract_segment_data_async(self, segment_basic_data):
        """
        Extract complete segment data: leaderboard + details + altitude profile
        
        Args:
            segment_basic_data (dict): Basic segment info from explore API
        
        Returns:
            dict: Complete segment data or None if extraction fails
        """
        segment_id = segment_basic_data["id"]
        segment_name = segment_basic_data.get("name", "Unknown")
        
        # Step 1: Scrape leaderboard times
        best_time, average_top_10, tenth_best = await self.leaderboard_extractor.get_times(segment_id)
        
        if best_time is None:
            print(f"  ✗ No leaderboard data")
            return None
        
        print(f"  ✓ Leaderboard: best={best_time}s, avg={average_top_10:.1f}s")
        
        # Step 2: Get segment details from API
        time.sleep(1.0)  # Rate limiting
        details = self.get_segment_details(segment_id)
        
        if not details:
            print(f"  ✗ Failed to get segment details")
            return None
        
        # Step 3: Get altitude profile from API
        time.sleep(0.2)  # Rate limiting
        streams = self.get_segment_streams(segment_id)
        
        # Combine all data
        return {
            "id": segment_id,
            "name": details.get("name"),
            "activity_type": details.get("activity_type"),
            "distance": details.get("distance"),
            "elevation_gain": details.get("total_elevation_gain"),
            "elevation_low": details.get("elevation_low"),
            "elevation_high": details.get("elevation_high"),
            "best_time": best_time,
            "average_top_10_time": round(average_top_10, 2) if average_top_10 else None,
            "tenth_best_time": tenth_best,
            "total_effort_count": details.get("effort_count"),
            "total_athlete_count": details.get("athlete_count"),
            "altitude_profile": streams.get("altitude", {}).get("data", []) if streams else [],
            "distance_profile": streams.get("distance", {}).get("data", []) if streams else [],
            "coordinates": streams.get("latlng", {}).get("data", []) if streams else []
        }
    
    def search_segments(self, max_segments=100):
        """
        Search for segments in a geographic area using grid-based approach
        
        Args:
            max_segments (int): Maximum number of segments to find
        
        Returns:
            list: List of segment basic data dictionaries
        """
        # Define search area (Reunion Island)
        lat_min, lat_max = -21.3980463, -20.8369424
        lng_min, lng_max = 55.4785652, 55.5090946
        
        all_segments = []
        segment_ids = set()
        grid_size = 20
        lat_step = (lat_max - lat_min) / grid_size
        lng_step = (lng_max - lng_min) / grid_size
        
        # Search for riding segments only
        for activity_type in ["riding"]:
            print(f"Searching for {activity_type} segments...")
            
            # Grid-based search
            for i in range(grid_size):
                for j in range(grid_size):
                    if len(segment_ids) >= max_segments:
                        break
                    
                    # Define grid cell bounds
                    bounds = [
                        lat_min + i * lat_step,
                        lng_min + j * lng_step,
                        lat_min + (i + 1) * lat_step,
                        lng_min + (j + 1) * lng_step
                    ]
                    
                    # Search in this grid cell
                    segments = self.explore_segments(bounds, activity_type=activity_type)
                    
                    # Add new segments
                    for seg in segments:
                        if seg["id"] not in segment_ids and len(segment_ids) < max_segments:
                            segment_ids.add(seg["id"])
                            all_segments.append(seg)
                    
                    time.sleep(0.3)  # Rate limiting
        
        return all_segments[:max_segments]
    
    def load_existing_data(self):
        """
        Load existing segments from saved files
        
        Returns:
            tuple: (list of segment dicts, set of segment IDs)
        """
        parquet_path = self.raw_folder / "reunion_segments.parquet"
        csv_path = self.raw_folder / "reunion_segments.csv"
        
        # Try loading from Parquet first (faster)
        if parquet_path.exists():
            try:
                df = pd.read_parquet(parquet_path)
                return df.to_dict('records'), set(df['id'].tolist())
            except Exception as e:
                print(f"Error loading parquet: {e}")
        
        # Fallback to CSV
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                return df.to_dict('records'), set(df['id'].tolist())
            except Exception as e:
                print(f"Error loading CSV: {e}")
        
        return [], set()
    
    def save_data(self, new_data):
        """
        Save segment data to Parquet and CSV files
        Merges with existing data to avoid duplicates
        
        Args:
            new_data (list): List of segment data dictionaries
        """
        if not new_data:
            print("No new data to save")
            return
        
        # Load existing data
        existing_data, existing_ids = self.load_existing_data()
        
        # Merge with new data (avoid duplicates)
        added_count = 0
        for segment in new_data:
            if segment['id'] not in existing_ids:
                existing_data.append(segment)
                existing_ids.add(segment['id'])
                added_count += 1
        
        if added_count == 0:
            print("No new segments added (all were duplicates)")
            return
        
        print(f"Added {added_count} new segments. Total: {len(existing_data)}")
        
        df = pd.DataFrame(existing_data)
        
        # Save as Parquet (full data including arrays)
        parquet_path = self.raw_folder / "reunion_segments.parquet"
        df.to_parquet(parquet_path, index=False)
        print(f"✓ Parquet saved: {parquet_path}")
        
        # Save as CSV (summary only, without array columns)
        csv_path = self.raw_folder / "reunion_segments.csv"
        summary_cols = [
            'id', 'name', 'activity_type', 'distance', 'elevation_gain',
            'elevation_low', 'elevation_high', 'best_time', 
            'average_top_10_time', 'tenth_best_time', 'total_effort_count', 
            'total_athlete_count'
        ]
        
        cols_to_save = [c for c in summary_cols if c in df.columns]
        df[cols_to_save].to_csv(csv_path, index=False, na_rep='')
        print(f"✓ CSV saved: {csv_path}")
    
    def number_of_processed_segments(self):
        """
        Count total number of processed segments
        
        Returns:
            int: Number of segments already saved
        """
        _, existing_ids = self.load_existing_data()
        return len(existing_ids)
    
    async def extract_all_data_async(self, max_segments):
        """
        Main extraction pipeline: search, filter, and process segments
        
        Args:
            max_segments (int): Target number of total segments
        
        Returns:
            list: List of newly extracted segment data dictionaries
        """
        print(f"Searching for up to {max_segments} segments...")
        
        # Step 1: Find segments via API (may raise RateLimitException)
        all_segments = self.search_segments(max_segments)
        
        # Step 2: Filter already processed segments
        _, existing_ids = self.load_existing_data()
        new_segments = [s for s in all_segments if s["id"] not in existing_ids]
        
        print(f"\nTotal found: {len(all_segments)}")
        print(f"  Already saved: {len(all_segments) - len(new_segments)}")
        print(f"  To process: {len(new_segments)}")
        
        if not new_segments:
            print("No new segments to process!")
            return []
        
        # Initialize browser for leaderboard scraping
        await self.init_browser()
        
        detailed_data = []
        
        try:
            # Step 3: Process each new segment
            for i, seg in enumerate(new_segments, 1):
                print(f"\nProcessing {i}/{len(new_segments)}: {seg.get('name')}")
                
                try:
                    data = await self.extract_segment_data_async(seg)
                    if data:
                        detailed_data.append(data)
                        
                except RateLimitException as e:
                    print(f"\n⚠️ {e}")
                    print(f"Saving {len(detailed_data)} segments collected so far...")
                    break  # Exit loop but save collected data
                    
        finally:
            await self.close_browser()
        
        return detailed_data


async def main():
    """Main execution with multi-account support and clean exit handling"""
    # Setup paths and configuration
    project_root = Path(__file__).resolve().parents[2]
    config_path = project_root / "config.yaml"
    
    # Initialize token manager
    token_manager = StravaTokenManager(config_path)
    print(f"✓ Using account: {token_manager.get_current_account()['name']}")
    
    # Initialize extractor
    extractor = StravaSegmentExtractor(token_manager)
    
    # Check existing progress
    nb_existing = extractor.number_of_processed_segments()
    print(f"Already processed segments: {nb_existing}")
    
    target_segments = nb_existing + 250
    
    # Initialize empty data list
    data = []
    
    try:
        # Run main extraction pipeline
        data = await extractor.extract_all_data_async(max_segments=target_segments)
        
    except RateLimitException as e:
        # Handle rate limit gracefully (data already collected is preserved)
        print(f"\n⚠️ Script terminated cleanly by rate limit: {e}")
        pass
    
    # Save collected data (even if interrupted)
    extractor.save_data(data)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"EXTRACTION SUMMARY")
    print(f"{'='*50}")
    print(f"New segments extracted: {len(data)}")
    print(f"Total processed: {extractor.number_of_processed_segments()}")
    token_manager.print_status()
    print(f"{'='*50}")


if __name__ == "__main__":
    asyncio.run(main())