import requests
import json
import time
import yaml
from datetime import datetime


        
class StravaSegmentExtractor:
    def __init__(self, access_token):
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
    
    def get_segment_leaderboard(self, segment_id, per_page=10):
        """Get leaderboard for a segment"""
        url = f"{self.base_url}/segments/{segment_id}/leaderboard"
        params = {"per_page": per_page}
        response = requests.get(url, headers=self.headers, params=params)
        time.sleep(0.2)  # Rate limiting
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting leaderboard for segment {segment_id}")
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
    
    def extract_segment_data(self, segment_id):
        """Extract all required data for a segment"""
        # Get segment details
        details = self.get_segment_details(segment_id)
        if not details:
            return None
        
        # Get leaderboard
        leaderboard = self.get_segment_leaderboard(segment_id, per_page=10)
        
        # Get altitude profile
        streams = self.get_segment_streams(segment_id)
        
        # Calculate average of top 10 times
        top_10_avg = None
        if leaderboard and "entries" in leaderboard:
            entries = leaderboard["entries"]
            if entries:
                times = [entry["elapsed_time"] for entry in entries]
                top_10_avg = sum(times) / len(times) if times else None
        
        # Compile data
        segment_data = {
            "id": segment_id,
            "name": details.get("name"),
            "activity_type": details.get("activity_type"),
            "distance": details.get("distance"),  # in meters
            "elevation_gain": details.get("total_elevation_gain"),  # in meters
            "elevation_low": details.get("elevation_low"),
            "elevation_high": details.get("elevation_high"),
            "best_time": leaderboard["entries"][0]["elapsed_time"] if leaderboard and leaderboard.get("entries") else None,
            "average_top_10_time": top_10_avg,
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
    
    def extract_all_data(self, max_segments=100):
        """Main function to extract all segment data"""
        print(f"Searching for up to {max_segments} segments in Reunion Island...")
        segments = self.search_reunion_segments(max_segments)
        
        print(f"Found {len(segments)} segments. Extracting detailed data...")
        detailed_data = []
        
        for i, seg in enumerate(segments, 1):
            print(f"Processing segment {i}/{len(segments)}: {seg.get('name')}")
            data = self.extract_segment_data(seg["id"])
            if data:
                detailed_data.append(data)
        
        return detailed_data
    
    def save_to_json(self, data, filename="reunion_segments.json"):
        """Save data to JSON file"""
        import os
        os.makedirs("data/raw", exist_ok=True)
        filepath = os.path.join("data/raw", filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to {filepath}")
    
    def save_to_csv(self, data, filename="reunion_segments.csv"):
        """Save data to CSV file (without altitude profile)"""
        import csv
        import os
        
        if not data:
            print("No data to save")
            return
        
        os.makedirs("data/raw", exist_ok=True)
        filepath = os.path.join("data/raw", filename)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'id', 'name', 'activity_type', 'distance', 'elevation_gain',
                'elevation_low', 'elevation_high', 'best_time', 
                'average_top_10_time', 'total_effort_count', 'total_athlete_count'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for segment in data:
                row = {k: segment.get(k) for k in fieldnames}
                writer.writerow(row)
        
        print(f"CSV data saved to {filepath}")


if __name__ == "__main__":
    
    with open('../config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    ACCESS_TOKEN = config['strava']['access_token']
    
    # Initialize extractor
    extractor = StravaSegmentExtractor(ACCESS_TOKEN)
    
    # Extract data for up to 100 segments
    data = extractor.extract_all_data(max_segments=100)
    
    # Save results
    extractor.save_to_json(data, "reunion_segments_full.json")
    extractor.save_to_csv(data, "reunion_segments_summary.csv")
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Total segments extracted: {len(data)}")
    if data:
        print(f"\nSample data from first segment:")
        sample = data[0]
        print(f"Name: {sample['name']}")
        print(f"Distance: {sample['distance']}m")
        print(f"Elevation gain: {sample['elevation_gain']}m")
        print(f"Elevation range: {sample['elevation_low']}m - {sample['elevation_high']}m")
        print(f"Best time: {sample['best_time']}s")
        print(f"Avg top 10: {sample['average_top_10_time']:.1f}s" if sample['average_top_10_time'] else "N/A")
        print(f"Total efforts: {sample['total_effort_count']}")
        print(f"Total athletes: {sample['total_athlete_count']}")
        print(f"Altitude profile points: {len(sample['altitude_profile'])}")