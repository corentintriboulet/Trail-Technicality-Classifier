import numpy as np
import pandas as pd


class SegmentSlicer:
    """
    Découpe un profil d'altitude en 5 types de sections :
    - flat : plat (< 1.5% de pente)
    - uphill : montée non catégorisée (1.5-3%)
    - climb : montée catégorisée Strava (Cat 4 à HC)
    - downhill : descente non catégorisée (-1.5% à -3%)
    - descent : descente catégorisée (avec comptage de virages)
    """
    
    def __init__(self):
        pass
    
    def cut_segment(self, altitude_profile, distance_profile, coordinates=None, 
                    smooth_window=10):
        """
        Découpe le profil en sections significatives.
        Adapte automatiquement la longueur minimale selon la distance totale.
        """
        # Créer DataFrame optimisé
        df = self._create_dataframe(altitude_profile, distance_profile, coordinates)
        
        if len(df) < 2:
            return []
        
        # Calcul vectorisé des grades et lissage
        df = self._compute_grades_and_smooth(df, smooth_window)
        
        # Adapter les seuils selon la longueur du segment
        total_distance = df['distance'].iloc[-1]
        min_length, min_elev = self._get_adaptive_thresholds(total_distance)
        
        # Détection des montées et descentes
        climbs = self._detect_slopes(df, direction='climb', min_length, min_elev)
        descents = self._detect_slopes(df, direction='descent', min_length, min_elev, coordinates)
        
        # Fusion et tri
        all_segments = climbs + descents
        all_segments.sort(key=lambda x: x['start_distance'])
        
        # Remplir les trous
        return self._fill_gaps(df, all_segments)
    
    def _create_dataframe(self, altitude_profile, distance_profile, coordinates):
        """Crée le DataFrame de manière optimisée"""
        # Gérer les coordonnées (numpy arrays)
        if coordinates is not None:
            try:
                lat_coords = np.array([float(c[0]) for c in coordinates])
                lon_coords = np.array([float(c[1]) for c in coordinates])
            except:
                lat_coords = np.zeros(len(altitude_profile))
                lon_coords = np.zeros(len(altitude_profile))
        else:
            lat_coords = np.zeros(len(altitude_profile))
            lon_coords = np.zeros(len(altitude_profile))
        
        return pd.DataFrame({
            'ele': altitude_profile,
            'distance': distance_profile,
            'lat': lat_coords,
            'lon': lon_coords
        })
    
    def _get_adaptive_thresholds(self, total_distance):
        """
        Adapte les seuils de détection selon la longueur totale du segment.
        Pour les segments < 1km : plus de détails avec des sections plus courtes.
        """
        if total_distance < 1000:  # < 1km
            min_length = 50   # Sections de 50m minimum
            min_elev = 10     # 10m de dénivelé minimum
        elif total_distance < 5000:  # < 5km
            min_length = 150
            min_elev = 15
        else:  # > 5km
            min_length = 300
            min_elev = 20
        
        return min_length, min_elev
    
    def _compute_grades_and_smooth(self, df, window):
        """Calcul vectorisé des grades et lissage (plus rapide)"""
        # Calcul vectorisé
        dist_diff = df['distance'].diff()
        elev_diff = df['ele'].diff()
        
        df['grade'] = np.where(dist_diff > 0, (elev_diff / dist_diff) * 100, 0)
        
        # Lissage
        window_size = min(max(3, window), len(df))
        if window_size % 2 == 0:
            window_size += 1
        
        df['plot_grade'] = df['grade'].rolling(
            window=window_size, center=True, min_periods=1
        ).mean()
        
        return df
    
    def _detect_slopes(self, df, direction='climb', min_length=300, min_elev=20, coordinates=None):
        """
        Détection unifiée des montées et descentes (code simplifié).
        direction: 'climb' ou 'descent'
        """
        # Configuration selon la direction
        if direction == 'climb':
            start_thresh = 3.0
            end_thresh = 1.5
            sign = 1
        else:  # descent
            start_thresh = -3.0
            end_thresh = -1.5
            sign = -1
        
        slopes = []
        state = "SEARCHING"
        start_idx = 0
        points = []
        
        for i in range(1, len(df)):
            slope = df.iloc[i]['plot_grade'] * sign
            elev_diff = (df['ele'].iloc[i] - df['ele'].iloc[i-1]) * sign
            dist_diff = df['distance'].iloc[i] - df['distance'].iloc[i-1]
            
            if state == "SEARCHING":
                if slope >= start_thresh * sign:
                    state = "IN_SLOPE"
                    start_idx = i - 1
                    points = [i-1, i]
            
            elif state == "IN_SLOPE":
                if slope >= end_thresh * sign:
                    points.append(i)
                else:
                    state = "PAUSE"
                    pause_idx = i - 1
                    pause_dist = 0
                    pause_elev = 0
                    points.append(i)
            
            elif state == "PAUSE":
                points.append(i)
                pause_dist += dist_diff
                if elev_diff < 0:
                    pause_elev += abs(elev_diff)
                
                if slope >= end_thresh * sign:
                    state = "IN_SLOPE"
                elif pause_dist > 200 or pause_elev > 15:
                    # Fin de la pente, valider et sauvegarder
                    segment_df = df.iloc[points[:-(i - pause_idx)]]
                    self._validate_and_append(slopes, segment_df, direction, 
                                             min_length, min_elev, coordinates)
                    state = "SEARCHING"
                    points = []
        
        # Dernière pente en cours
        if state in ["IN_SLOPE", "PAUSE"] and len(points) > 0:
            segment_df = df.iloc[points]
            self._validate_and_append(slopes, segment_df, direction, 
                                     min_length, min_elev, coordinates)
        
        return slopes
    
    def _validate_and_append(self, slopes_list, segment_df, direction, 
                            min_length, min_elev, coordinates):
        """Valide et ajoute une montée/descente à la liste"""
        if segment_df.empty or len(segment_df) < 2:
            return
        
        length = segment_df['distance'].iloc[-1] - segment_df['distance'].iloc[0]
        
        if direction == 'climb':
            elev_change = segment_df[segment_df['ele'].diff() > 0]['ele'].diff().sum()
            if pd.isna(elev_change):
                elev_change = 0
            avg_slope = (elev_change / length) * 100 if length > 0 else 0
        else:
            elev_change = abs(segment_df[segment_df['ele'].diff() < 0]['ele'].diff().sum())
            if pd.isna(elev_change):
                elev_change = 0
            avg_slope = -(elev_change / length) * 100 if length > 0 else 0
        
        if length < min_length or elev_change < min_elev:
            return
        
        # Classification
        if direction == 'climb':
            category = self._classify_strava(length, avg_slope)
            segment_type = "climb" if category != "Uncategorized" else "uphill"
            sharp_turns = 0
        else:
            category = self._classify_strava(length, abs(avg_slope))
            segment_type = "descent" if category != "Uncategorized" else "downhill"
            sharp_turns = self._count_sharp_turns(segment_df, coordinates) if segment_type == "descent" else 0
        
        slopes_list.append({
            'type': segment_type,
            'category': category,
            'start_distance': segment_df['distance'].iloc[0],
            'end_distance': segment_df['distance'].iloc[-1],
            'distance': length,
            'start_altitude': segment_df['ele'].iloc[0],
            'end_altitude': segment_df['ele'].iloc[-1],
            'elevation_gain': elev_change if direction == 'climb' else 0,
            'elevation_loss': elev_change if direction == 'descent' else 0,
            'elevation_change': elev_change if direction == 'climb' else -elev_change,
            'grade': avg_slope,
            'max_grade': segment_df['plot_grade'].max(),
            'min_grade': segment_df['plot_grade'].min(),
            'grade_variance': segment_df['plot_grade'].var(),
            'sharp_turns': sharp_turns,
            'start_idx': segment_df.index[0],
            'end_idx': segment_df.index[-1]
        })
    
    def _classify_strava(self, length_m, avg_slope):
        """Classification Strava unifiée (montées et descentes)"""
        if avg_slope < 3.0:
            return "Uncategorized"
        
        score = length_m * avg_slope
        
        if score >= 80000:
            return "HC"
        elif score >= 64000:
            return "Cat 1"
        elif score >= 32000:
            return "Cat 2"
        elif score >= 16000:
            return "Cat 3"
        elif score >= 8000:
            return "Cat 4"
        else:
            return "Uncategorized"
    
    def _count_sharp_turns(self, segment_df, coordinates):
        """Compte les virages serrés (angle > 60° sur < 50m)"""
        if coordinates is None or len(segment_df) < 3:
            return 0
        
        try:
            start_idx = segment_df.index[0]
            end_idx = segment_df.index[-1]
            
            if end_idx >= len(coordinates) or start_idx >= len(coordinates):
                return 0
            
            coords = coordinates[start_idx:end_idx+1]
            sharp_turns = 0
            
            for i in range(1, len(coords) - 1):
                # Vecteurs entre 3 points consécutifs
                v1 = np.array([float(coords[i-1][0]) - float(coords[i][0]),
                              float(coords[i-1][1]) - float(coords[i][1])])
                v2 = np.array([float(coords[i+1][0]) - float(coords[i][0]),
                              float(coords[i+1][1]) - float(coords[i][1])])
                
                # Calcul de l'angle
                norm1 = np.linalg.norm(v1)
                norm2 = np.linalg.norm(v2)
                
                if norm1 > 0 and norm2 > 0:
                    cos_angle = np.clip(np.dot(v1, v2) / (norm1 * norm2), -1, 1)
                    angle = np.degrees(np.arccos(cos_angle))
                    
                    # Distance entre les points
                    if i < len(segment_df) - 1:
                        dist = segment_df['distance'].iloc[i+1] - segment_df['distance'].iloc[i-1]
                        
                        if angle > 60 and dist < 50:
                            sharp_turns += 1
            
            return sharp_turns
        except:
            return 0
    
    def _fill_gaps(self, df, segments):
        """Remplit les espaces entre segments détectés"""
        if not segments:
            avg_grade = df['plot_grade'].mean()
            return [self._create_flat_segment(df, 0, len(df)-1, avg_grade)]
        
        filled = []
        last_end = 0
        
        for seg in segments:
            # Gap avant ce segment
            if seg['start_distance'] > last_end + 50:
                gap_start = df[df['distance'] >= last_end].index[0]
                gap_end = df[df['distance'] <= seg['start_distance']].index[-1]
                
                gap_seg = self._create_flat_segment(
                    df, gap_start, gap_end,
                    df.loc[gap_start:gap_end, 'plot_grade'].mean()
                )
                
                if gap_seg['distance'] > 50:
                    filled.append(gap_seg)
            
            filled.append(seg)
            last_end = seg['end_distance']
        
        # Gap final
        if last_end < df['distance'].iloc[-1] - 50:
            gap_start = df[df['distance'] >= last_end].index[0]
            gap_end = len(df) - 1
            
            gap_seg = self._create_flat_segment(
                df, gap_start, gap_end,
                df.loc[gap_start:gap_end, 'plot_grade'].mean()
            )
            
            if gap_seg['distance'] > 50:
                filled.append(gap_seg)
        
        return filled
    
    def _create_flat_segment(self, df, start_idx, end_idx, avg_grade):
        """Crée un segment flat/uphill/downhill"""
        segment_df = df.iloc[start_idx:end_idx+1]
        
        length = segment_df['distance'].iloc[-1] - segment_df['distance'].iloc[0]
        elev_change = segment_df['ele'].iloc[-1] - segment_df['ele'].iloc[0]
        
        # Déterminer le type
        if avg_grade > 1.5:
            seg_type = "uphill"
        elif avg_grade < -1.5:
            seg_type = "downhill"
        else:
            seg_type = "flat"
        
        return {
            'type': seg_type,
            'category': "Uncategorized",
            'start_distance': segment_df['distance'].iloc[0],
            'end_distance': segment_df['distance'].iloc[-1],
            'distance': length,
            'start_altitude': segment_df['ele'].iloc[0],
            'end_altitude': segment_df['ele'].iloc[-1],
            'elevation_gain': max(0, elev_change),
            'elevation_loss': max(0, -elev_change),
            'elevation_change': elev_change,
            'grade': avg_grade,
            'max_grade': segment_df['plot_grade'].max(),
            'min_grade': segment_df['plot_grade'].min(),
            'grade_variance': segment_df['plot_grade'].var(),
            'sharp_turns': 0,
            'start_idx': start_idx,
            'end_idx': end_idx
        }