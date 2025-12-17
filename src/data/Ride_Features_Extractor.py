import pandas as pd
from src.data.Segment_Slicer import cut_segment 
from src.data.Ride_Look_Up_Table_Generator import compute_segment_time_fast

import sys, os, pickle
import numpy as np
from pathlib import Path
repo_root = Path().resolve().parent  
sys.path.append(str(repo_root))

class RideFeaturesExtractor:
    pass

    def generate_sections_dict(self, df):
        sections_dict= {}
        for idx, row in df.iterrows():
            segment_id = row['segment_id']
            # Charger altitude_profile, distance_profile, coordinates
            sections = cut_segment(row['altitude_profile'], row['distance_profile'], row['coordinates'])
            sections_dict[segment_id] = sections
    
        return sections_dict
    
    def load_sections_dict(self, file_path):
        import pickle
        with open(file_path, 'rb') as f:
            sections_dict = pickle.load(f)
        return sections_dict
    
    def get_sections_dict(self, df):
        ## if file exists, load it
        file_path = repo_root / 'data' / 'processed' / 'sections_dict_ride.pkl'
        if os.path.exists(file_path):
            self.sections_dict = self.load_sections_dict(file_path)
        else:
            self.sections_dict = self.generate_sections_dict(df)
        return self.sections_dict
    
    def extract_features_from_sections(self, sections, segment_id=None, df=None):
        """
        Extrait des features simples à partir des sections d'un segment.
        
        Input: sections (list of dict) - output de segment_slicer.cut_segment()
        Output: dict of features
        """
        
        if not sections or len(sections) == 0:
            return None
        
        # ========== Features Basiques ==========
        total_distance = sum(s['distance'] for s in sections) / 1000  # en km
        total_elevation_gain = sum(s['elevation_gain'] for s in sections)
        total_elevation_loss = sum(s['elevation_loss'] for s in sections)
        
        # Grades
        all_grades = [s['grade'] for s in sections]
        avg_grade = np.mean(all_grades)
        max_grade = max(s['max_grade'] for s in sections)
        min_grade = min(s['min_grade'] for s in sections)
        
        # ========== Features d'Ordre (Capture la Séquence) ==========
        
        # 1. Distribution des montées (early vs late)
        early_third_distance = total_distance * 1000 * 0.33
        late_third_distance = total_distance * 1000 * 0.67
        
        early_climb_gain = sum(s['elevation_gain'] for s in sections 
                            if s['start_distance'] < early_third_distance)
        late_climb_gain = sum(s['elevation_gain'] for s in sections 
                            if s['start_distance'] > late_third_distance)
        
        early_climb_ratio = early_climb_gain / (total_elevation_gain + 1e-6)
        late_climb_ratio = late_climb_gain / (total_elevation_gain + 1e-6)
        
        # 2. Grade pondéré par position (effet fatigue)
        weighted_grade = 0
        for i, s in enumerate(sections):
            position_weight = 1 + (i / len(sections)) * 0.5  # 1.0 à 1.5x
            weighted_grade += s['grade'] * position_weight * s['distance']
        weighted_grade /= (total_distance * 1000)
        
        # 3. Position de la section la plus dure
        hardest_idx = np.argmax([s['grade'] * s['distance'] for s in sections])
        hardest_section_position = hardest_idx / len(sections)  # 0 à 1
        
        # 4. Variabilité du terrain
        grade_variance = np.mean([s['grade_variance'] for s in sections])
        
        # 5. Stats par tiers du segment
        first_third = [s for s in sections if s['start_distance'] < early_third_distance]
        middle_third = [s for s in sections 
                        if early_third_distance <= s['start_distance'] <= late_third_distance]
        last_third = [s for s in sections if s['start_distance'] > late_third_distance]
        
        first_third_avg_grade = np.mean([s['grade'] for s in first_third]) if first_third else 0
        middle_third_avg_grade = np.mean([s['grade'] for s in middle_third]) if middle_third else 0
        last_third_avg_grade = np.mean([s['grade'] for s in last_third]) if last_third else 0
        
        # 6. Compter les types de sections
        n_climbs = sum(1 for s in sections if s['type'] in ['climb', 'uphill'])
        n_descents = sum(1 for s in sections if s['type'] in ['descent', 'downhill'])
        n_flats = sum(1 for s in sections if s['type'] == 'flat')

        # 7. Features lié au data leakage
        best_time = df.loc[df['segment_id'] == segment_id, 'best_time'].iloc[0]
        avg_top_10_time = df.loc[df['segment_id'] == segment_id, 'average_top_10_time'].iloc[0]
        total_effort_count = df.loc[df['segment_id'] == segment_id, 'total_effort_count'].iloc[0]
        inv_total_effort_count = 1 / (total_effort_count + 1e-6)  

        # 8. Distance par catégorie de montée
        cat_hc_distance = sum(s['distance'] for s in sections if s['category'] == 'Cat HC') / 1000  # en km
        cat_1_distance = sum(s['distance'] for s in sections if s['category'] == 'Cat 1') / 1000  # en km
        cat_2_distance = sum(s['distance'] for s in sections if s['category'] == 'Cat 2') / 1000  # en km
        cat_3_distance = sum(s['distance'] for s in sections if s['category'] == 'Cat 3') / 1000  # en km  
        cat_4_distance = sum(s['distance'] for s in sections if s['category'] == 'Cat 4') / 1000  # en km
        
        uphill_distance = sum(s['distance'] for s in sections if s['type'] == 'uphill') / 1000  # en km
        downhill_distance = sum(s['distance'] for s in sections if s['type'] == 'downhill') / 1000  # en km
        flat_distance = sum(s['distance'] for s in sections if s['type'] == 'flat') / 1000  # en km

        # 9. Score physique
        with open('../src/lookup_tables/250W_2900Ws_05_power_time_LUT_ride.pkl', 'rb') as f:
            lookup_dict = pickle.load(f)
        time_score = compute_segment_time_fast(sections, total_distance, lookup_dict=lookup_dict)

        
            
        
        # ========== Assembler le dictionnaire de features ==========
        features = {
            # Basiques
            'total_distance_km': total_distance,
            'total_elevation_gain': total_elevation_gain,
            'total_elevation_loss': total_elevation_loss,
            #'avg_grade': avg_grade,
            'max_grade': max_grade,
            'min_grade': min_grade,
            'grade_variance': grade_variance,
            
            # Ordre et fatigue
            'early_climb_ratio': early_climb_ratio,
            'late_climb_ratio': late_climb_ratio,
            'weighted_grade': weighted_grade,
            'hardest_section_position': hardest_section_position,
            
            # Tiers
            'first_third_avg_grade': first_third_avg_grade,
            'middle_third_avg_grade': middle_third_avg_grade,
            'last_third_avg_grade': last_third_avg_grade,
            
            # Comptages
            #'n_sections': len(sections),
            #'n_climbs': n_climbs,
            #'n_descents': n_descents,
            #'n_flats': n_flats,

            # Data leakage
            #'best_time': best_time,
            #'avg_top_10_time': avg_top_10_time,
            #'total_effort_count': total_effort_count,
            #'inv_total_effort_count': inv_total_effort_count

            # Distance par catégorie de montée
            #'cat_hc_distance_km': cat_hc_distance,
            'cat_1_distance_km': cat_1_distance,
            'cat_2_distance_km': cat_2_distance,
            'cat_3_distance_km': cat_3_distance,
            'cat_4_distance_km': cat_4_distance,
            'uphill_distance_km': uphill_distance,
            'downhill_distance_km': downhill_distance,
            'flat_distance_km': flat_distance,

            # Polynomial features 
            'flat_cat_4_interaction': flat_distance * cat_4_distance,
            'flat_cat_3_interaction': flat_distance * cat_4_distance,
            'flat_cat_2_interaction': flat_distance * cat_4_distance,
            'flat_cat_1_interaction': flat_distance * cat_4_distance,
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
            
            # Physics scores
            'time_score': time_score,


        }
        
        return features
    
    def extract_features_for_dataframe(self, df, sections_dict):
        features_list = []
        segment_ids = []  # ← AJOUTER
        
        for idx, row in df.iterrows():
            segment_id = row['segment_id']
            
            if segment_id not in sections_dict:
                print(f"Warning: No sections found for segment {segment_id}")
                continue
            
            sections = sections_dict[segment_id]
            features = self.extract_features_from_sections(sections, segment_id=segment_id, df=df)
            
            if features is not None:
                features_list.append(features)
                segment_ids.append(segment_id)  # ← AJOUTER
        
        features_df = pd.DataFrame(features_list, index=segment_ids)  # ← MODIFIER
        print(f"✓ Extracted features for {len(features_df)} / {len(df)} segments")
        
        return features_df
    
    def get_features_dataframe(self, df):
        sections_dict = self.get_sections_dict(df)
        features_df = self.extract_features_for_dataframe(df, sections_dict)
        return features_df
    
    def cleaning_features_dataframe(self, df, features_df, sections_dict):
        df = df.copy()
        mask_too_fast_best_rider = (df['best_time'] < df['average_top_10_time'] / 2)
        df.loc[mask_too_fast_best_rider, 'best_time'] = df.loc[mask_too_fast_best_rider, 'average_top_10_time'].astype(int)

        mask_too_steep_slope = (features_df['max_grade'] <= 30) & (features_df['min_grade'] >= -30)
        print(f"Removing {len(features_df) - mask_too_steep_slope.sum()} outliers from training set based on grade thresholds.")
        features_df = features_df.loc[mask_too_steep_slope]
        sections_dict = {k: v for k, v in sections_dict.items() if k in df['segment_id'].values}
        
        return df, features_df, sections_dict
    
    def get_features_dataframe_cleaned(self, df, features_df, sections_dict):
        features_df = self.get_features_dataframe(df)
        df_cleaned, features_df_cleaned, sections_dict_cleaned = self.cleaning_features_dataframe(df, features_df, sections_dict)
        return features_df_cleaned, df_cleaned