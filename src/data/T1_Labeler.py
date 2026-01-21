import sys, re, os
import pandas as pd
import numpy as np
from pathlib import Path
from scipy.spatial import cKDTree
repo_root = Path().resolve().parent  
sys.path.append(str(repo_root))


class T1Labeler:
    """
    Labels segments as T1 (technical level 1 - roads/paved surfaces) or non-T1 (trails/technical terrain)
    Supports both Ride and Run activity types
    """
    
    def __init__(self):
        pass
        
    def detect_t1_segments(self, df, sections_dict, activity_type, confidence_threshold='medium'):
        """
        Detects T1 segments with a confidence score.
        
        Args:
            df: DataFrame with segment data
            sections_dict: Dictionary of sections for each segment
            activity_type: 'Ride' or 'Run'
            confidence_threshold: 'high', 'medium', or 'low'
        
        Returns:
            DataFrame with columns: is_T1_predicted, T1_confidence_score, T1_reasons
        """
        
        df = df.copy()
        
        # Initialize columns
        df['T1_score'] = 0.0
        df['T1_reasons'] = ''
        
        # Criterion 1: Segment name
        name_scores = df.apply(lambda row: self._score_from_name(row['name']), axis=1)
        df['T1_score'] += name_scores
        
        # Criterion 2: Elevation profile (regularity)
        elevation_scores = df.apply(
            lambda row: self._score_from_elevation_profile(
                row['altitude_profile'], 
                row['distance_profile']
            ), 
            axis=1
        )
        df['T1_score'] += elevation_scores
        
        # Criterion 3: Sections (grade variance)
        section_scores = []
        for idx, row in df.iterrows():
            if row['segment_id'] in sections_dict:
                score = self._score_from_sections(sections_dict[row['segment_id']])
            else:
                score = 0
            section_scores.append(score)
        df['T1_score'] += section_scores
        
        # Criterion 4: Speed ratio (comparison with similar segments)
        speed_scores = self._score_from_speed_comparison(df)
        df['T1_score'] += speed_scores
        
        # Normalize score (0-10)
        max_possible_score = 4.0  # 4 criteria × max 1.0 each
        df['T1_confidence_score'] = (df['T1_score'] / max_possible_score) * 10
        
        # Define threshold
        thresholds = {
            'high': 7.0,    # Very confident
            'medium': 5.5,  # Medium confidence
            'low': 4.0      # Low confidence but possible
        }
        
        threshold = thresholds[confidence_threshold]
        df['is_T1_predicted'] = df['T1_confidence_score'] >= threshold
        
        # Statistics
        n_detected = df['is_T1_predicted'].sum()
        
        return df  
    
    def _score_from_name(self, name):
        """
        Score based on segment name.
        Returns: 0.0 to 1.0
        """
        
        if pd.isna(name):
            return 0.3  # Neutral if no name
        
        name_lower = name.lower()
        
        # ROAD keywords (T1)
        road_keywords = [
            r'\broute\b', r'\brn\d+\b', r'\bd\d+\b', r'\brte\b',  # National/departmental road
            r'\brd\b', r'\bav\b', r'\bavenue\b',
            r'\brue\b', r'\bchemin goudronn',
            r'\basphalte\b', r'\bbitume\b',
            r'\bcyclable\b', r'\bpiste cyclable\b',
            r'\bvoie verte\b',
            r'\bcol\b.*\broute\b', 
            r'\bstade\b', 
        ]
        
        # TRAIL keywords (NOT T1)
        trail_keywords = [
            r'\btrail\b', r'\bsentier\b', r'\bchemin\b',
            r'\bsingle\b', r'\bsingletrack\b',
            r'\btechnique\b', r'\bcailloux\b', r'\bracines\b',
            r'\bforest\b', r'\bforêt\b', r'\bbois\b',
            r'\bmontagne\b', r'\balpin\b',
            r'\bgr\d+\b',  # GR20, etc.
            r'\bcombe\b', r'\bravin\b',
        ]
        
        # Count matches
        road_matches = sum(1 for kw in road_keywords if re.search(kw, name_lower))
        trail_matches = sum(1 for kw in trail_keywords if re.search(kw, name_lower))
        
        # Score
        if road_matches > 0 and trail_matches == 0:
            return 1.0  # Clearly road
        elif road_matches > trail_matches:
            return 0.7  # Probably road
        elif trail_matches > 0 and road_matches == 0:
            return 0.0  # Clearly trail
        elif trail_matches > road_matches:
            return 0.2  # Probably trail
        else:
            return 0.3  # Ambiguous
    
    def _score_from_elevation_profile(self, altitude_profile, distance_profile):
        """
        Roads = more regular profile, less micro-variations.
        Trails = chaotic profile with many small variations.
        
        Returns: 0.0 to 1.0
        """
        
        if altitude_profile is None or len(altitude_profile) < 10:
            return 0.5  # Neutral
        
        # Calculate grade variance over small windows
        grades = []
        for i in range(1, len(altitude_profile)):
            dist_diff = distance_profile[i] - distance_profile[i-1]
            alt_diff = altitude_profile[i] - altitude_profile[i-1]
            if dist_diff > 0:
                grade = (alt_diff / dist_diff) * 100
                grades.append(grade)
        
        if len(grades) < 5:
            return 0.5
        
        # Local variance (window of 5 points)
        local_variances = []
        window = 5
        for i in range(len(grades) - window):
            local_var = np.var(grades[i:i+window])
            local_variances.append(local_var)
        
        avg_local_variance = np.mean(local_variances) if local_variances else 0
        
        # Roads: low variance (< 5)
        # Trails: high variance (> 20)
        if avg_local_variance < 3:
            return 1.0  # Very regular = road
        elif avg_local_variance < 8:
            return 0.7  # Fairly regular
        elif avg_local_variance < 15:
            return 0.4  # Medium
        else:
            return 0.0  # Chaotic = trail

    def _score_from_sections(self, sections):
        """
        Analyzes segment sections.
        Roads: fewer sections, low grade_variance.
        Trails: many sections, high grade_variance.
        
        Returns: 0.0 to 1.0
        """
        
        if not sections or len(sections) == 0:
            return 0.5
        
        # Average grade_variance
        avg_variance = np.mean([s['grade_variance'] for s in sections])
        
        # Number of sections per km
        total_distance_km = sum(s['distance'] for s in sections) / 1000
        sections_per_km = len(sections) / total_distance_km if total_distance_km > 0 else 0
        
        # Score based on variance
        if avg_variance < 2:
            variance_score = 1.0
        elif avg_variance < 5:
            variance_score = 0.7
        elif avg_variance < 10:
            variance_score = 0.4
        else:
            variance_score = 0.0
        
        # Score based on section density
        if sections_per_km < 2:
            density_score = 1.0  # Few sections = regular
        elif sections_per_km < 4:
            density_score = 0.6
        else:
            density_score = 0.2  # Many sections = irregular
        
        return (variance_score + density_score) / 2
    
    def _score_from_speed_comparison(self, df):
        """
        Compares speed with similar segments.
        Roads = faster at equivalent profile.
        
        Returns: Series of scores 0.0 to 1.0
        """
        
        # Calculate average speed
        df_temp = df.copy()
        df_temp['avg_speed'] = df_temp['distance'] / df_temp['best_time']
        
        # Normalize by difficulty (distance + elevation)
        df_temp['difficulty'] = df_temp['distance'] / 1000 + df_temp['elevation_gain'] / 10
        df_temp['normalized_speed'] = df_temp['avg_speed'] / df_temp['difficulty']
        
        # For each segment, compare with segments of similar difficulty
        scores = []
        
        for idx, row in df_temp.iterrows():
            # Find similar segments (±20% in difficulty)
            similar_mask = (
                (df_temp['difficulty'] >= row['difficulty'] * 0.8) &
                (df_temp['difficulty'] <= row['difficulty'] * 1.2) &
                (df_temp.index != idx)
            )
            
            if similar_mask.sum() > 5:
                similar_speeds = df_temp.loc[similar_mask, 'normalized_speed']
                percentile = (similar_speeds < row['normalized_speed']).sum() / len(similar_speeds)
                
                # If in top 30% of speeds → probably road
                if percentile > 0.8:
                    scores.append(1.0)
                elif percentile > 0.6:
                    scores.append(0.7)
                elif percentile > 0.4:
                    scores.append(0.5)
                else:
                    scores.append(0.3)
            else:
                scores.append(0.5)  # Not enough similar segments
        
        return pd.Series(scores, index=df.index)

    def augment_run_t1_from_rides(self, df_run, df_ride_t1, tolerance_meters=50, coverage_threshold=0.75):
        """
        Transfère les labels T1 des segments Vélo vers les segments Course à pied
        Utilise directement les coordonnées GPS avec correction de latitude
        """
        from scipy.spatial import cKDTree
        
        print("LOG: Starting Cross-Activity Label Transfer (Ride -> Run)...")
        
        df_run = df_run.copy()
        df_run['is_T1_geometric_match'] = False
        df_run['T1_matched_ride_id'] = None
        df_run['match_coverage_ratio'] = 0.0  # NOUVEAU: ratio de couverture
        
        # Conversion simple : degrés → mètres pour La Réunion
        METERS_PER_DEGREE_LAT = 111139  # constant
        METERS_PER_DEGREE_LON = 111139 * np.cos(np.radians(-21.1))  # ~103,700m
        
        # Tolérance en degrés
        tolerance_deg_lat = tolerance_meters / METERS_PER_DEGREE_LAT
        tolerance_deg_lon = tolerance_meters / METERS_PER_DEGREE_LON
        
        def coords_to_array(coords_input):
            """Simple extraction des coordonnées"""
            if coords_input is None:
                return None
            try:
                if isinstance(coords_input, np.ndarray) and coords_input.ndim == 2:
                    return coords_input
                else:
                    return np.vstack([np.array(c).flatten() for c in coords_input]).reshape(-1, 2)
            except:
                return None
        
        # 1. Indexer les Rides T1
        ride_spatial_index = []
        
        for idx, row in df_ride_t1.iterrows():
            coords = coords_to_array(row['coordinates'])
            
            if coords is not None and len(coords) > 0:
                # Bounding box en degrés GPS directs
                min_lat, min_lon = coords.min(axis=0)
                max_lat, max_lon = coords.max(axis=0)
                
                bbox = (
                    min_lat - tolerance_deg_lat,
                    min_lon - tolerance_deg_lon,
                    max_lat + tolerance_deg_lat,
                    max_lon + tolerance_deg_lon
                )
                
                # KDTree avec coordonnées GPS normalisées
                # Normaliser pour compenser la distorsion longitude
                coords_normalized = coords.copy()
                coords_normalized[:, 1] = coords_normalized[:, 1] * (METERS_PER_DEGREE_LON / METERS_PER_DEGREE_LAT)
                
                ride_spatial_index.append({
                    'id': row.get('segment_id', row.get('id')),
                    'name': row.get('name', 'Unknown'),  # NOUVEAU: nom du segment
                    'bbox': bbox,
                    'kdtree': cKDTree(coords_normalized),
                    'coords_raw': coords
                })
        
        print(f"LOG: Indexed {len(ride_spatial_index)} T1 Ride segments.")
        
        if len(ride_spatial_index) == 0:
            return df_run, []
        
        # 2. Matcher les segments Run
        matches_found = 0
        match_details = []  # NOUVEAU: liste des correspondances détaillées
        
        for idx, row in df_run.iterrows():
            run_coords = coords_to_array(row['coordinates'])
            
            if run_coords is None or len(run_coords) == 0:
                continue
            
            run_segment_id = row.get('segment_id', row.get('id'))
            run_name = row.get('name', 'Unknown')
            
            r_min_lat, r_min_lon = run_coords.min(axis=0)
            r_max_lat, r_max_lon = run_coords.max(axis=0)
            
            # Filtre bounding box
            candidates = []
            for ride in ride_spatial_index:
                b = ride['bbox']
                if (r_min_lat <= b[2] and r_max_lat >= b[0] and
                    r_min_lon <= b[3] and r_max_lon >= b[1]):
                    candidates.append(ride)
            
            if not candidates:
                continue
            
            # Normaliser les coordonnées Run
            run_coords_normalized = run_coords.copy()
            run_coords_normalized[:, 1] = run_coords_normalized[:, 1] * (METERS_PER_DEGREE_LON / METERS_PER_DEGREE_LAT)
            
            # Vérification géométrique
            for ride in candidates:
                # Calculer distances en degrés normalisés
                distances_normalized, _ = ride['kdtree'].query(run_coords_normalized)
                
                # Convertir en mètres (approximation)
                distances_meters = distances_normalized * METERS_PER_DEGREE_LAT
                
                points_on_road = np.sum(distances_meters <= tolerance_meters)
                match_ratio = points_on_road / len(run_coords)
                
                # NOUVEAU: Sauvegarder les détails même si pas un match parfait
                match_details.append({
                    'run_segment_id': run_segment_id,
                    'run_segment_name': run_name,
                    'ride_segment_id': ride['id'],
                    'ride_segment_name': ride['name'],
                    'coverage_ratio': match_ratio,
                    'points_on_road': points_on_road,
                    'total_run_points': len(run_coords),
                    'tolerance_meters': tolerance_meters,
                    'is_match': match_ratio >= coverage_threshold
                })
                
                if match_ratio >= coverage_threshold:
                    df_run.at[idx, 'is_T1_geometric_match'] = True
                    df_run.at[idx, 'T1_matched_ride_id'] = ride['id']
                    df_run.at[idx, 'match_coverage_ratio'] = match_ratio
                    matches_found += 1
                    break
        
        print(f"LOG: Found {matches_found} geometric matches.")
        
        # NOUVEAU: Sauvegarder le CSV des correspondances
        if match_details:
            matches_df = pd.DataFrame(match_details)
            # Trier par ratio décroissant pour voir les meilleurs matches en premier
            matches_df = matches_df.sort_values('coverage_ratio', ascending=False)
            
            output_path = repo_root / 'data' / 'processed' / 'geometric_matches_run_ride.csv'
            matches_df.to_csv(output_path, index=False)
            print(f"LOG: Saved {len(matches_df)} potential matches to {output_path}")
        
        return df_run, match_details
    
    def _get_geometric_t1_matches(self, df, activity_type):
        """
        Helper function to retrieve geometric T1 matches.
        Strategy: 
        1. Identify T1 Ride IDs from the CSV.
        2. Load the actual geometry (coordinates) from the master Parquet file (Source of Truth).
        3. Perform the geometric matching.
        """
        if activity_type != 'Run':
            return pd.DataFrame()
        
        # Paths
        ride_t1_csv_path = repo_root / 'data' / 'processed' / 'T1_active_learning_threshold_5_ride.csv'
        master_parquet_path = repo_root / 'data' / 'raw' / 'reunion_segments.parquet'
        
        if not os.path.exists(ride_t1_csv_path):
            print("LOG: Warning - T1 Ride CSV not found. Skipping geometric augmentation.")
            return pd.DataFrame()
            
        print(f"LOG: Loading T1 Ride segments for geometric cross-matching...")
        
        try:
            # 1. Get the list of Ride IDs that are T1
            df_ride_t1_ids = pd.read_csv(ride_t1_csv_path)
            
            # Handle potential column name mismatch (id vs segment_id)
            # Based on your CSV, it likely has 'id' or 'segment_id'
            id_col = 'segment_id' if 'segment_id' in df_ride_t1_ids.columns else 'id'
            if id_col not in df_ride_t1_ids.columns:
                 print(f"LOG: Error - Column '{id_col}' not found in CSV. columns: {df_ride_t1_ids.columns}")
                 return pd.DataFrame()

            target_ids = df_ride_t1_ids[id_col].unique()
            
            # 2. Load the geometry from the Parquet Source of Truth
            if os.path.exists(master_parquet_path):
                # Only load necessary columns to save memory
                df_master = pd.read_parquet(master_parquet_path, columns=['id', 'coordinates'])
                
                # Filter: Keep only the segments that are in our T1 list
                # Note: Parquet usually has 'id', ensure we match the right column
                df_ride_geometry = df_master[df_master['id'].isin(target_ids)].copy()
                
                # Rename 'id' to 'segment_id' for consistency if needed
                if 'segment_id' not in df_ride_geometry.columns:
                    df_ride_geometry['segment_id'] = df_ride_geometry['id']
                    
                print(f"LOG: Loaded geometry for {len(df_ride_geometry)} T1 rides from Parquet.")
            else:
                print("LOG: Master Parquet not found. Cannot perform robust geometric matching.")
                return pd.DataFrame()

            # 3. Apply the matching logic
            df_run_all = df[df['activity_type'] == 'Run'].copy()

            df_run_augmented, match_details = self.augment_run_t1_from_rides(  # MODIFIÉ
                df_run_all, 
                df_ride_geometry,
                tolerance_meters=50,
                coverage_threshold=0.75
            )

            # 4. Return only the matches
            matches = df_run_augmented[df_run_augmented['is_T1_geometric_match'] == True].copy()
            print(f"LOG: Geometric matching found {len(matches)} new segments")
            return matches
            
        except Exception as e:
            print(f"LOG: Error during geometric matching: {e}")
            # import traceback
            # traceback.print_exc()
            return pd.DataFrame()

    def get_t1_labeled_segments_from_labeler(self, df, sections_dict, activity_type, threshold=5):
        """
        Gets T1 segments detected by the labeler or loads from cache.
        """
        
        if activity_type == 'Ride':
            file_name = f'T1_active_learning_threshold_{threshold}_ride.csv'
            confidence = 'medium'
        elif activity_type == 'Run':
            file_name = f'T1_active_learning_threshold_{threshold}_run.csv'
            confidence = 'high'
        else:
            raise ValueError(f"Invalid activity_type: {activity_type}")
        
        file_path = repo_root / 'data' / 'processed' / file_name
        
        if os.path.exists(file_path):
            labeled_segments = pd.read_csv(file_path)
            
            # AJOUT : Filtrer seulement les segments T1 prédits !
            if 'is_T1_predicted' in labeled_segments.columns:
                labeled_segments = labeled_segments[labeled_segments['is_T1_predicted'] == True].copy()
            
            print(f"LOG: Loaded {len(labeled_segments)} T1 {activity_type} segments from labeler")
        else:
            print(f"LOG: Detecting T1 {activity_type} segments using active learning...")
            labeled_segments = self.detect_t1_segments(
                df,
                sections_dict=sections_dict,
                activity_type=activity_type,
                confidence_threshold=confidence
            )

            labeled_segments = labeled_segments[labeled_segments['is_T1_predicted'] == True].copy()
            labeled_segments = labeled_segments[['segment_id']].copy()
            labeled_segments.to_csv(file_path, index=False)
            
            
            print(f"LOG: Detected {len(labeled_segments)} T1 {activity_type} segments")
        
        return labeled_segments
    
    def get_t1_manually_labeled_segments(self, df, activity_type):
        """
        Loads manually labeled T1 segments from file.
        
        Args:
            df: DataFrame with segment data
            activity_type: 'Ride' or 'Run'
        
        Returns:
            T1_segments_manually_labeled: Segments manually labeled as T1
            not_T1_segments_manually_labeled: Segments manually labeled as NOT T1
        """
        
        file_name = 'segments_manually_labeled.csv'
        file_path = repo_root / 'data' / 'processed' / file_name
        
        if os.path.exists(file_path):
            manual_labels = pd.read_csv(file_path)
            
            # Filter by activity type
            if activity_type == 'Ride':
                activity_segments = df[df['activity_type'] == 'Ride'].index
            elif activity_type == 'Run':
                activity_segments = df[df['activity_type'] == 'Run'].index
            else:
                raise ValueError(f"Invalid activity_type: {activity_type}")
            
            T1_segments_manually_labeled = manual_labels[
                (manual_labels['technicality'] == 1) & 
                (manual_labels['segment_id'].isin(activity_segments))
            ].copy()
            
            not_T1_segments_manually_labeled = manual_labels[
                (manual_labels['technicality'] != 1) & 
                (manual_labels['segment_id'].isin(activity_segments))
            ].copy()
        else:
            raise FileNotFoundError(f"Manual T1 labels file not found at {file_path}")
        
        return T1_segments_manually_labeled, not_T1_segments_manually_labeled
    
    def get_t1_labeled_segments(self, df, sections_dict, activity_type, threshold=None):
        """
        Combines predicted and manually labeled T1 segments.
        
        Args:
            df: DataFrame with segment data
            sections_dict: Dictionary of sections
            activity_type: 'Ride' or 'Run'
            threshold: Confidence threshold (default 5 for Ride, 7 for Run)
        
        Returns:
            DataFrame: Segments with T1 labels (predicted + manual)
        """
        
        # Set default threshold based on activity type
        if threshold is None:
            threshold = 5 if activity_type == 'Ride' else 7
        
        # Get predicted T1 segments
        df_T1_predicted = self.get_t1_labeled_segments_from_labeler(
            df, sections_dict, activity_type, threshold=threshold
        )
        
        # Get manually labeled segments
        T1_segments_manually_labeled, not_T1_segments_manually_labeled = \
            self.get_t1_manually_labeled_segments(df, activity_type)
        
        # Get geometric T1 matches (for Run only)
        df_T1_geometric = self._get_geometric_t1_matches(df, activity_type)

        # Combine predicted and manual labels
        T1_combined = pd.concat([
            df_T1_predicted, 
            T1_segments_manually_labeled,
            df_T1_geometric
        ]).drop_duplicates(subset=['segment_id']).reset_index(drop=True)
        
        # Remove segments manually labeled as NOT T1
        T1_combined = T1_combined.drop(
            T1_combined[
                T1_combined['segment_id'].isin(not_T1_segments_manually_labeled['segment_id'])
            ].index
        ).reset_index(drop=True)
        
        print(f"LOG: Total T1 {activity_type} segments after combining predicted and manual labels: {len(T1_combined)}")
        
        return T1_combined