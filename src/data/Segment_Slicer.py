import numpy as np
import pandas as pd

class SegmentSlicer:
    def __init__(self):
        pass
    def cut_segment(self, altitude_profile, distance_profile, coordinates=None,
                    smooth_window=10):
        """
        Découpe un profil en 5 types de sections:
        - flat: plat
        - uphill: montée non catégorisée
        - climb: montée catégorisée (Cat 4 à HC)
        - downhill: descente non catégorisée
        - descent: descente catégorisée (basée sur longueur/pente)
        """
        
        # Gérer les coordonnées (qui peuvent être des arrays numpy)
        has_coords = coordinates is not None
        if has_coords:
            try:
                lat_coords = [float(c[0]) for c in coordinates]
                lon_coords = [float(c[1]) for c in coordinates]
            except:
                has_coords = False
                lat_coords = [0] * len(altitude_profile)
                lon_coords = [0] * len(altitude_profile)
        else:
            lat_coords = [0] * len(altitude_profile)
            lon_coords = [0] * len(altitude_profile)
        
        # Créer un DataFrame pour faciliter le traitement
        df = pd.DataFrame({
            'ele': altitude_profile,
            'distance': distance_profile,
            'lat': lat_coords,
            'lon': lon_coords
        })
        
        if len(df) < 2:
            return []
        
        # Calculer les grades point par point
        df = self._calculate_grades(df)
        
        # Appliquer le lissage
        df = self._apply_smoothing(df, smooth_window)
        
        # Détection des segments significatifs
        climbs = self._detect_climbs(df)
        descents = self._detect_descents(df, coordinates)
        
        # Fusionner et trier tous les segments
        all_segments = climbs + descents
        all_segments.sort(key=lambda x: x['start_distance'])
        
        # Remplir les trous avec des sections "flat", "uphill" ou "downhill"
        filled_segments = self._fill_gaps(df, all_segments)
        
        return filled_segments

    def _calculate_grades(self, df):
        """Calcule les grades entre chaque point"""
        dist_diff = df['distance'].diff()
        elev_diff = df['ele'].diff()
        
        df['grade'] = np.where(
            dist_diff > 0,
            (elev_diff / dist_diff) * 100,
            0
        )
        
        return df

    def _apply_smoothing(self, df, window):
        """Applique un lissage sur les grades"""
        window_size = min(window, len(df))
        if window_size % 2 == 0:
            window_size += 1
        
        df['plot_grade'] = df['grade'].rolling(
            window=window_size, 
            center=True, 
            min_periods=1
        ).mean()
        
        return df

    def _detect_climbs(self, df, 
                    start_threshold=3.0,
                    end_threshold=1.5,
                    max_pause_length_m=200,
                    max_pause_descent_m=15,
                    min_length_m=300,
                    min_gain_m=20):
        """
        Détecte les montées significatives (climb) et non catégorisées (uphill)
        Retourne une liste de dictionnaires avec les caractéristiques de chaque montée
        """
        
        climbs = []
        state = "SEARCHING"
        start_idx = 0
        segment_points = []
        
        for i in range(1, len(df)):
            point = df.iloc[i]
            slope = point['plot_grade']
            elev_diff = df['ele'].iloc[i] - df['ele'].iloc[i-1]
            dist_diff = point['distance'] - df['distance'].iloc[i-1]
            
            if state == "SEARCHING":
                if slope >= start_threshold:
                    state = "IN_CLIMB"
                    start_idx = i - 1
                    segment_points = [df.iloc[i-1].to_dict(), point.to_dict()]
                    
            elif state == "IN_CLIMB":
                if slope >= end_threshold:
                    segment_points.append(point.to_dict())
                else:
                    state = "EVALUATING_PAUSE"
                    pause_start_idx = i - 1
                    pause_length = 0
                    pause_descent = 0
                    segment_points.append(point.to_dict())
                    
            elif state == "EVALUATING_PAUSE":
                segment_points.append(point.to_dict())
                pause_length += dist_diff
                
                if elev_diff < 0:
                    pause_descent += abs(elev_diff)
                
                # La montée reprend
                if slope >= end_threshold:
                    state = "IN_CLIMB"
                
                # La pause est trop longue ou descend trop
                elif pause_length > max_pause_length_m or pause_descent > max_pause_descent_m:
                    # Sauvegarder le segment avant la pause
                    segment_df = pd.DataFrame(segment_points[:-(i - pause_start_idx)])
                    self._validate_and_append_climb(climbs, segment_df, start_idx, 
                                            min_length_m, min_gain_m)
                    
                    state = "SEARCHING"
                    segment_points = []
        
        # Dernière montée en cours
        if state in ["IN_CLIMB", "EVALUATING_PAUSE"] and segment_points:
            segment_df = pd.DataFrame(segment_points)
            self._validate_and_append_climb(climbs, segment_df, start_idx, 
                                    min_length_m, min_gain_m)
        
        return climbs

    def _validate_and_append_climb(self, climbs_list, segment_df, start_idx, 
                                min_length_m, min_gain_m):
        """Valide et ajoute une montée à la liste"""
        
        if segment_df.empty or len(segment_df) < 2:
            return
        
        length = segment_df['distance'].iloc[-1] - segment_df['distance'].iloc[0]
        gain = segment_df[segment_df['ele'].diff() > 0]['ele'].diff().sum()
        
        if pd.isna(gain):
            gain = 0
        
        if length < min_length_m or gain < min_gain_m:
            return
        
        avg_slope = (gain / length) * 100 if length > 0 else 0
        max_grade = segment_df['plot_grade'].max()
        
        # Classifier selon Strava
        category = self._classify_climb_strava(length, avg_slope)
        
        # Déterminer le type: "climb" (catégorisé) ou "uphill" (non catégorisé)
        segment_type = "climb" if category != "Uncategorized" else "uphill"
        
        climbs_list.append({
            'type': segment_type,
            'category': category,
            'start_distance': segment_df['distance'].iloc[0],
            'end_distance': segment_df['distance'].iloc[-1],
            'distance': length,
            'start_altitude': segment_df['ele'].iloc[0],
            'end_altitude': segment_df['ele'].iloc[-1],
            'elevation_gain': gain,
            'elevation_loss': 0,
            'elevation_change': gain,
            'grade': avg_slope,
            'max_grade': max_grade,
            'min_grade': segment_df['plot_grade'].min(),
            'grade_variance': segment_df['plot_grade'].var(),
            'start_idx': start_idx,
            'end_idx': start_idx + len(segment_df) - 1
        })

    def _detect_descents(self, df, coordinates=None,
                        start_threshold=-3.0,
                        end_threshold=-1.5,
                        max_pause_length_m=200,
                        max_pause_ascent_m=15,
                        min_length_m=300,
                        min_loss_m=20):
        """
        Détecte les descentes significatives (descent) et non catégorisées (downhill)
        Ajoute le nombre de virages serrés pour les descentes catégorisées
        """
        
        descents = []
        state = "SEARCHING"
        start_idx = 0
        segment_points = []
        
        for i in range(1, len(df)):
            point = df.iloc[i]
            slope = point['plot_grade']
            elev_diff = df['ele'].iloc[i] - df['ele'].iloc[i-1]
            dist_diff = point['distance'] - df['distance'].iloc[i-1]
            
            if state == "SEARCHING":
                if slope <= start_threshold:
                    state = "IN_DESCENT"
                    start_idx = i - 1
                    segment_points = [df.iloc[i-1].to_dict(), point.to_dict()]
                    
            elif state == "IN_DESCENT":
                if slope <= end_threshold:
                    segment_points.append(point.to_dict())
                else:
                    state = "EVALUATING_PAUSE"
                    pause_start_idx = i - 1
                    pause_length = 0
                    pause_ascent = 0
                    segment_points.append(point.to_dict())
                    
            elif state == "EVALUATING_PAUSE":
                segment_points.append(point.to_dict())
                pause_length += dist_diff
                
                if elev_diff > 0:
                    pause_ascent += elev_diff
                
                # La descente reprend
                if slope <= end_threshold:
                    state = "IN_DESCENT"
                
                # La pause est trop longue ou monte trop
                elif pause_length > max_pause_length_m or pause_ascent > max_pause_ascent_m:
                    segment_df = pd.DataFrame(segment_points[:-(i - pause_start_idx)])
                    self._validate_and_append_descent(descents, segment_df, start_idx, 
                                                min_length_m, min_loss_m, coordinates)
                    
                    state = "SEARCHING"
                    segment_points = []
        
        # Dernière descente en cours
        if state in ["IN_DESCENT", "EVALUATING_PAUSE"] and segment_points:
            segment_df = pd.DataFrame(segment_points)
            self._validate_and_append_descent(descents, segment_df, start_idx, 
                                        min_length_m, min_loss_m, coordinates)
        
        return descents

    def _validate_and_append_descent(self, descents_list, segment_df, start_idx, 
                                    min_length_m, min_loss_m, coordinates):
        """Valide et ajoute une descente à la liste"""
        
        if segment_df.empty or len(segment_df) < 2:
            return
        
        length = segment_df['distance'].iloc[-1] - segment_df['distance'].iloc[0]
        loss = abs(segment_df[segment_df['ele'].diff() < 0]['ele'].diff().sum())
        
        if pd.isna(loss):
            loss = 0
        
        if length < min_length_m or loss < min_loss_m:
            return
        
        avg_slope = -(loss / length) * 100 if length > 0 else 0
        min_grade = segment_df['plot_grade'].min()
        
        # Classifier la descente (similaire aux montées)
        category = self._classify_descent(length, abs(avg_slope))
        
        # Déterminer le type
        segment_type = "descent" if category != "Uncategorized" else "downhill"
        
        # Compter les virages serrés seulement pour les descentes catégorisées
        sharp_turns = 0
        if segment_type == "descent" and coordinates:
            sharp_turns = self._count_sharp_turns(segment_df, coordinates)
        
        descents_list.append({
            'type': segment_type,
            'category': category,
            'start_distance': segment_df['distance'].iloc[0],
            'end_distance': segment_df['distance'].iloc[-1],
            'distance': length,
            'start_altitude': segment_df['ele'].iloc[0],
            'end_altitude': segment_df['ele'].iloc[-1],
            'elevation_gain': 0,
            'elevation_loss': loss,
            'elevation_change': -loss,
            'grade': avg_slope,
            'max_grade': segment_df['plot_grade'].max(),
            'min_grade': min_grade,
            'grade_variance': segment_df['plot_grade'].var(),
            'sharp_turns': sharp_turns if segment_type == "descent" else 0,
            'start_idx': start_idx,
            'end_idx': start_idx + len(segment_df) - 1
        })

    def _classify_climb_strava(self, length_m, avg_slope):
        """Classification Strava pour les montées"""
        
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

    def _classify_descent(self, length_m, avg_slope):
        """
        Classification pour les descentes (similaire aux montées)
        Basée sur longueur et pente moyenne
        """
        
        if avg_slope < 3.0:
            return "Uncategorized"
        
        score = length_m * avg_slope
        
        if score >= 64000:
            return "Major Descent"
        elif score >= 32000:
            return "Significant Descent"
        elif score >= 16000:
            return "Moderate Descent"
        elif score >= 8000:
            return "Minor Descent"
        else:
            return "Uncategorized"

    def _count_sharp_turns(self, segment_df, coordinates):
        """
        Compte les virages serrés dans une descente
        Un virage serré est défini comme un changement d'angle > 60° sur < 50m
        """
        
        if not coordinates or len(segment_df) < 3:
            return 0
        
        sharp_turns = 0
        
        # Extraire les coordonnées du segment
        start_idx = segment_df.index[0]
        end_idx = segment_df.index[-1]
        
        if end_idx >= len(coordinates) or start_idx >= len(coordinates):
            return 0
        
        segment_coords = coordinates[start_idx:end_idx+1]
        
        for i in range(1, len(segment_coords) - 1):
            # Calculer l'angle entre 3 points consécutifs
            angle = self._calculate_angle(
                segment_coords[i-1],
                segment_coords[i],
                segment_coords[i+1]
            )
            
            # Distance entre les points
            if i < len(segment_df) - 1:
                dist = segment_df['distance'].iloc[i+1] - segment_df['distance'].iloc[i-1]
                
                # Virage serré: angle > 60° sur moins de 50m
                if angle > 60 and dist < 50:
                    sharp_turns += 1
        
        return sharp_turns

    def _calculate_angle(self, p1, p2, p3):
        """
        Calcule l'angle formé par 3 points (en degrés)
        p1, p2, p3 sont des tuples (lat, lon)
        """
        
        # Vecteurs
        v1 = (p1[0] - p2[0], p1[1] - p2[1])
        v2 = (p3[0] - p2[0], p3[1] - p2[1])
        
        # Produit scalaire et normes
        dot_product = v1[0] * v2[0] + v1[1] * v2[1]
        norm1 = np.sqrt(v1[0]**2 + v1[1]**2)
        norm2 = np.sqrt(v2[0]**2 + v2[1]**2)
        
        if norm1 == 0 or norm2 == 0:
            return 0
        
        # Angle en radians puis en degrés
        cos_angle = dot_product / (norm1 * norm2)
        cos_angle = np.clip(cos_angle, -1, 1)  # Éviter erreurs d'arrondi
        angle_rad = np.arccos(cos_angle)
        angle_deg = np.degrees(angle_rad)
        
        return angle_deg

    def _fill_gaps(self, df, segments):
        """
        Remplit les espaces entre les segments détectés avec:
        - flat: si pente proche de 0
        - uphill: si pente positive modérée
        - downhill: si pente négative modérée
        """
        
        if not segments:
            # Tout le parcours est un seul segment
            avg_grade = df['plot_grade'].mean()
            return [self._create_flat_segment(df, 0, len(df)-1, avg_grade)]
        
        filled = []
        last_end_dist = 0
        
        for seg in segments:
            # Remplir l'espace avant ce segment
            if seg['start_distance'] > last_end_dist + 50:  # Gap de plus de 50m
                gap_start_idx = df[df['distance'] >= last_end_dist].index[0]
                gap_end_idx = df[df['distance'] <= seg['start_distance']].index[-1]
                
                gap_segment = self._create_flat_segment(
                    df, gap_start_idx, gap_end_idx,
                    df.loc[gap_start_idx:gap_end_idx, 'plot_grade'].mean()
                )
                
                if gap_segment['distance'] > 50:  # Ignorer les très petits gaps
                    filled.append(gap_segment)
            
            filled.append(seg)
            last_end_dist = seg['end_distance']
        
        # Remplir après le dernier segment
        if last_end_dist < df['distance'].iloc[-1] - 50:
            gap_start_idx = df[df['distance'] >= last_end_dist].index[0]
            gap_end_idx = len(df) - 1
            
            gap_segment = self._create_flat_segment(
                df, gap_start_idx, gap_end_idx,
                df.loc[gap_start_idx:gap_end_idx, 'plot_grade'].mean()
            )
            
            if gap_segment['distance'] > 50:
                filled.append(gap_segment)
        
        return filled

    def _create_flat_segment(self, df, start_idx, end_idx, avg_grade):
        """Crée un segment flat/uphill/downhill basé sur la pente moyenne"""
        
        start_dist = df['distance'].iloc[start_idx]
        end_dist = df['distance'].iloc[end_idx]
        
        start_alt = df['ele'].iloc[start_idx]
        end_alt = df['ele'].iloc[end_idx]
        
        length = end_dist - start_dist
        elev_change = end_alt - start_alt
        
        if avg_grade > 1:
            seg_type = "uphill"
        elif avg_grade < -1:
            seg_type = "downhill"
        else:
            seg_type = "flat"
        
        return {
            "type": seg_type,
            "category": "Uncategorized",
            "start_distance": start_dist,
            "end_distance": end_dist,
            "distance": length,
            "start_altitude": start_alt,
            "end_altitude": end_alt,
            "elevation_gain": max(0, elev_change),
            "elevation_loss": max(0, -elev_change),
            "elevation_change": elev_change,
            "grade": avg_grade,
            "max_grade": df['plot_grade'].iloc[start_idx:end_idx+1].max(),
            "min_grade": df['plot_grade'].iloc[start_idx:end_idx+1].min(),
            "grade_variance": df['plot_grade'].iloc[start_idx:end_idx+1].var(),
            "start_idx": start_idx,
            "end_idx": end_idx,
        }