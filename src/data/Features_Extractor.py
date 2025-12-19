"""
Features Extractor

Extracts terrain features from Strava segments for both cycling and running activities.
Uses Segment Slicer to break down elevation profiles into categorized sections,
then computes comprehensive terrain metrics including:
- Basic metrics (distance, elevation, grades)
- Sequential features (early/late climb ratios, position-weighted grades)
- Category-based distances (Cat 1-4, HC climbs)
- Physics-based time scores using lookup tables

Supports both Ride and Run activities with activity-specific lookup tables.
"""

import pandas as pd
import numpy as np
import sys
import os
import pickle
from pathlib import Path

from src.data.Segment_Slicer import SegmentSlicer
from src.data.Ride_Look_Up_Table_Generator import RideLookUpTableGenerator
from src.data.Run_Look_Up_Table_Generator import RunLookUpTableGenerator

# Initialize utilities
segment_slicer = SegmentSlicer()
ride_lut_generator = RideLookUpTableGenerator()
run_lut_generator = RunLookUpTableGenerator()

# Setup paths
repo_root = Path().resolve().parent
sys.path.append(str(repo_root))


class FeaturesExtractor:
    """Extract terrain features from segments for both Ride and Run activities"""
    
    def __init__(self):
        self.sections_dict = None
    
    def filter_by_activity(self, df, activity_type):
        """
        Filter DataFrame by activity type
        
        Args:
            df (DataFrame): Segment data
            activity_type (str): 'Ride' or 'Run'
        
        Returns:
            DataFrame: Filtered data for specified activity type
        """
        if activity_type not in ['Ride', 'Run']:
            raise ValueError("activity_type must be 'Ride' or 'Run'")
        
        return df[df['activity_type'] == activity_type].copy()
    
    def generate_sections_dict(self, df):
        """
        Generate sections dictionary for all segments
        
        Args:
            df (DataFrame): Segment data with altitude_profile, distance_profile, coordinates
        
        Returns:
            dict: {segment_id: sections_list}
        """
        sections_dict = {}
        for idx, row in df.iterrows():
            segment_id = row['segment_id']
            sections = segment_slicer.cut_segment(
                row['altitude_profile'], 
                row['distance_profile'], 
                row['coordinates']
            )
            sections_dict[segment_id] = sections
        
        return sections_dict
    
    def load_sections_dict(self, file_path):
        """Load sections dictionary from pickle file"""
        with open(file_path, 'rb') as f:
            sections_dict = pickle.load(f)
        return sections_dict
    
    def save_sections_dict(self, sections_dict, file_path):
        """Save sections dictionary to pickle file"""
        with open(file_path, 'wb') as f:
            pickle.dump(sections_dict, f)
        print(f"LOG: Sections dictionary saved to {file_path}")
    
    def get_sections_dict(self, df, activity_type):
        """
        Get sections dictionary (load from cache or generate)
        
        Args:
            df (DataFrame): Segment data
            activity_type (str): 'Ride' or 'Run'
        
        Returns:
            dict: Sections dictionary
        """
        # Determine cache file path
        file_name = f'sections_dict_{activity_type.lower()}.pkl'
        file_path = repo_root / 'data' / 'processed' / file_name
        
        # Load from cache if exists
        if os.path.exists(file_path):
            print(f"LOG: Loading sections dictionary from {file_path}")
            self.sections_dict = self.load_sections_dict(file_path)
        else:
            print(f"LOG: Generating sections dictionary for {activity_type}...")
            self.sections_dict = self.generate_sections_dict(df)
            self.save_sections_dict(self.sections_dict, file_path)
        
        return self.sections_dict
    
    def extract_features_from_sections(self, sections, segment_id, df, activity_type):
        """
        Extract terrain features from segment sections
        
        Args:
            sections (list): List of section dictionaries from SegmentSlicer
            segment_id (int): Segment ID for lookup
            df (DataFrame): Full segment data for metadata lookup
            activity_type (str): 'Ride' or 'Run'
        
        Returns:
            dict: Feature dictionary or None if invalid
        """
        if not sections or len(sections) == 0:
            return None
        
        # ========== Basic Features ==========
        total_distance = sum(s['distance'] for s in sections) / 1000  # km
        total_elevation_gain = sum(s['elevation_gain'] for s in sections)
        total_elevation_loss = sum(s['elevation_loss'] for s in sections)
        
        # Grades
        all_grades = [s['grade'] for s in sections]
        max_grade = max(s['max_grade'] for s in sections)
        min_grade = min(s['min_grade'] for s in sections)
        
        # ========== Sequential Features (Capture Order) ==========
        
        # 1. Early vs late climb distribution
        early_third_distance = total_distance * 1000 * 0.33
        late_third_distance = total_distance * 1000 * 0.67
        
        early_climb_gain = sum(
            s['elevation_gain'] for s in sections 
            if s['start_distance'] < early_third_distance
        )
        late_climb_gain = sum(
            s['elevation_gain'] for s in sections 
            if s['start_distance'] > late_third_distance
        )
        
        early_climb_ratio = early_climb_gain / (total_elevation_gain + 1e-6)
        late_climb_ratio = late_climb_gain / (total_elevation_gain + 1e-6)
        
        # 2. Position-weighted grade (fatigue effect)
        weighted_grade = 0
        for i, s in enumerate(sections):
            position_weight = 1 + (i / len(sections)) * 0.5  # 1.0 to 1.5x
            weighted_grade += s['grade'] * position_weight * s['distance']
        weighted_grade /= (total_distance * 1000)
        
        # 3. Hardest section position
        hardest_idx = np.argmax([s['grade'] * s['distance'] for s in sections])
        hardest_section_position = hardest_idx / len(sections)  # 0 to 1
        
        # 4. Terrain variability
        grade_variance = np.mean([s['grade_variance'] for s in sections])
        
        # 5. Stats by thirds
        first_third = [s for s in sections if s['start_distance'] < early_third_distance]
        middle_third = [
            s for s in sections 
            if early_third_distance <= s['start_distance'] <= late_third_distance
        ]
        last_third = [s for s in sections if s['start_distance'] > late_third_distance]
        
        first_third_avg_grade = np.mean([s['grade'] for s in first_third]) if first_third else 0
        middle_third_avg_grade = np.mean([s['grade'] for s in middle_third]) if middle_third else 0
        last_third_avg_grade = np.mean([s['grade'] for s in last_third]) if last_third else 0
        
        # ========== Distance by Category ==========
        cat_1_distance = sum(s['distance'] for s in sections if s['category'] == 'Cat 1') / 1000
        cat_2_distance = sum(s['distance'] for s in sections if s['category'] == 'Cat 2') / 1000
        cat_3_distance = sum(s['distance'] for s in sections if s['category'] == 'Cat 3') / 1000
        cat_4_distance = sum(s['distance'] for s in sections if s['category'] == 'Cat 4') / 1000
        
        uphill_distance = sum(s['distance'] for s in sections if s['type'] == 'uphill') / 1000
        downhill_distance = sum(s['distance'] for s in sections if s['type'] == 'downhill') / 1000
        flat_distance = sum(s['distance'] for s in sections if s['type'] == 'flat') / 1000
        
        # ========== Physics-Based Time Score ==========
        # Load activity-specific lookup table
        if activity_type == 'Ride':
            lut_path = '../src/lookup_tables/250W_2900Ws_05_power_time_LUT_ride.pkl'
            with open(lut_path, 'rb') as f:
                lookup_dict = pickle.load(f)
            time_score = ride_lut_generator.compute_segment_time_fast(
                sections, total_distance, lookup_dict=lookup_dict
            )
        else:  # Run
            lut_path = '../src/lookup_tables/60ml_kg_min_6m_s_distance_time_LUT_run.pkl'
            with open(lut_path, 'rb') as f:
                lookup_dict = pickle.load(f)
            time_score = run_lut_generator.compute_segment_time_fast(
                sections, total_distance, lookup_dict=lookup_dict
            )
        
        # ========== Assemble Feature Dictionary ==========
        features = {
            # Basic metrics
            'total_distance_km': total_distance,
            'total_elevation_gain': total_elevation_gain,
            'total_elevation_loss': total_elevation_loss,
            'max_grade': max_grade,
            'min_grade': min_grade,
            'grade_variance': grade_variance,
            
            # Sequential features
            'early_climb_ratio': early_climb_ratio,
            'late_climb_ratio': late_climb_ratio,
            'weighted_grade': weighted_grade,
            'hardest_section_position': hardest_section_position,
            
            # Thirds statistics
            'first_third_avg_grade': first_third_avg_grade,
            'middle_third_avg_grade': middle_third_avg_grade,
            'last_third_avg_grade': last_third_avg_grade,
            
            # Distance by category
            'cat_1_distance_km': cat_1_distance,
            'cat_2_distance_km': cat_2_distance,
            'cat_3_distance_km': cat_3_distance,
            'cat_4_distance_km': cat_4_distance,
            'uphill_distance_km': uphill_distance,
            'downhill_distance_km': downhill_distance,
            'flat_distance_km': flat_distance,
            
            # Interaction features (polynomial)
            'flat_cat_4_interaction': flat_distance * cat_4_distance,
            'flat_cat_3_interaction': flat_distance * cat_3_distance,
            'flat_cat_2_interaction': flat_distance * cat_2_distance,
            'flat_cat_1_interaction': flat_distance * cat_1_distance,
            'flat_DH_interaction': flat_distance * downhill_distance,
            'flat_UH_interaction': flat_distance * uphill_distance,
            'UH_cat_4_interaction': uphill_distance * cat_4_distance,
            'UH_cat_3_interaction': uphill_distance * cat_3_distance,
            'UH_cat_2_interaction': uphill_distance * cat_2_distance,
            'UH_cat_1_interaction': uphill_distance * cat_1_distance,
            'DH_cat_4_interaction': downhill_distance * cat_4_distance,
            'DH_cat_3_interaction': downhill_distance * cat_3_distance,
            'DH_cat_2_interaction': downhill_distance * cat_2_distance,
            'DH_cat_1_interaction': downhill_distance * cat_1_distance,
            
            # Physics score
            'time_score': time_score,
        }
        
        return features
    
    def extract_features_for_dataframe(self, df, sections_dict, activity_type):
        """
        Extract features for all segments in DataFrame
        
        Args:
            df (DataFrame): Segment data
            sections_dict (dict): Pre-computed sections
            activity_type (str): 'Ride' or 'Run'
        
        Returns:
            DataFrame: Features with segment_id as index
        """
        features_list = []
        segment_ids = []
        
        for idx, row in df.iterrows():
            segment_id = row['segment_id']
            
            if segment_id not in sections_dict:
                print(f"LOG: Warning: No sections found for segment {segment_id}")
                continue
            
            sections = sections_dict[segment_id]
            features = self.extract_features_from_sections(
                sections, segment_id=segment_id, df=df, activity_type=activity_type
            )
            
            if features is not None:
                features_list.append(features)
                segment_ids.append(segment_id)
        
        features_df = pd.DataFrame(features_list, index=segment_ids)
        print(f"LOG: Extracted features for {len(features_df)} / {len(df)} segments")
        
        return features_df
    
    def get_features_dataframe(self, df, activity_type):
        """
        Get features DataFrame (convenience method)
        
        Args:
            df (DataFrame): Segment data
            activity_type (str): 'Ride' or 'Run'
        
        Returns:
            DataFrame: Features
        """
        sections_dict = self.get_sections_dict(df, activity_type)
        features_df = self.extract_features_for_dataframe(df, sections_dict, activity_type)
        return features_df
    
    def cleaning_features_dataframe(self, df, features_df, sections_dict, activity_type):
        """
        Clean outliers from features
        
        Args:
            df (DataFrame): Segment data
            features_df (DataFrame): Features
            sections_dict (dict): Sections dictionary
            activity_type (str): 'Ride' or 'Run'
        
        Returns:
            tuple: (cleaned_df, cleaned_features_df, cleaned_sections_dict)
        """
        df = df.copy()
        
        # Remove suspiciously fast best times
        mask_too_fast = (df['best_time'] < df['average_top_10_time'] / 2)
        df.loc[mask_too_fast, 'best_time'] = df.loc[mask_too_fast, 'average_top_10_time'].astype(int)
        
        # Remove extreme grades (Ride only - Run can have steeper grades)
        if activity_type == 'Ride':
            mask_valid_grade = (features_df['max_grade'] <= 30) & (features_df['min_grade'] >= -30)
            removed_count = len(features_df) - mask_valid_grade.sum()
            print(f"LOG: Removing {removed_count} outliers based on grade thresholds (|grade| > 30%)")
            features_df = features_df.loc[mask_valid_grade]
        
        # Clean sections dict
        sections_dict = {k: v for k, v in sections_dict.items() if k in df['segment_id'].values}
        
        return df, features_df, sections_dict
    
    def get_features_dataframe_cleaned(self, df, activity_type):
        """
        Complete pipeline: filter, extract, clean
        
        Args:
            df (DataFrame): Raw segment data
            activity_type (str): 'Ride' or 'Run'
        
        Returns:
            tuple: (features_df_cleaned, df_cleaned, sections_dict_cleaned)
        """
        # Filter by activity type
        df_filtered = self.filter_by_activity(df, activity_type)
        
        # Extract features
        features_df = self.get_features_dataframe(df_filtered, activity_type)
        sections_dict = self.get_sections_dict(df_filtered, activity_type)
        
        # Clean outliers
        df_cleaned, features_df_cleaned, sections_dict_cleaned = self.cleaning_features_dataframe(
            df_filtered, features_df, sections_dict, activity_type
        )
        
        return features_df_cleaned, df_cleaned, sections_dict_cleaned