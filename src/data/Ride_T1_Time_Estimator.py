import sys
from pathlib import Path
current_script_path = Path(__file__).resolve()
repo_root = current_script_path.parents[2]
sys.path.append(str(repo_root))
print(f"LOG: Repo root added to sys.path: {repo_root}")

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly import express as px
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, learning_curve, LearningCurveDisplay, RandomizedSearchCV
from sklearn.feature_selection import SequentialFeatureSelector
from sklearn.pipeline import Pipeline
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.ensemble import RandomForestRegressor, AdaBoostRegressor, GradientBoostingRegressor, StackingRegressor
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.linear_model import Ridge, ElasticNet,  ElasticNetCV, RidgeCV, LassoCV, SGDRegressor, LassoLarsCV, LinearRegression
import matplotlib.pyplot as plt
from Segment_Slicer import SegmentSlicer 
SegmentSlicer = SegmentSlicer()
from scipy.optimize import fsolve
from scipy.interpolate import interp1d
import pickle

class T1TimeEstimator():
    """
    Stage 2: Estimates T1 time using terrain features
    Uses terrain features to predict T1 time
    """
    
    def __init__(self):
        return None
    
    def load_segment(self, file_path):

        raw_df= pd.read_parquet(file_path)
        df= raw_df.copy()

        ride_df = df[df['activity_type']=='Ride'].copy()

        sections_dict_ride = {}
        for idx, row in ride_df.iterrows():
            segment_id = row['segment_id']
            # Charger altitude_profile, distance_profile, coordinates
            sections = SegmentSlicer.cut_segment(row['altitude_profile'], row['distance_profile'], row['coordinates'])
            sections_dict_ride[segment_id] = sections
        
        return ride_df, sections_dict_ride
    
    def extract_features(self, ride_df, sections_dict_ride):
        def compute_segment_time_fast(sections, segment_distance_km, lookup_dict):
            """
            Calcule le temps total RAPIDEMENT en utilisant la lookup table.
            
            Args:
                sections: Liste de dict avec 'distance' (m) et 'grade' (%)
                segment_distance_km: Distance totale du segment en km
                lookup_dict: Dictionnaire retourné par build_lookup_table_3d()
            
            Returns:
                float: Temps total en secondes
            """
            interpolator = lookup_dict['interpolator']
            
            total_time = 0.0
            
            for section in sections:
                sect_dist_km = section['distance'] / 1000  # convertir en km
                grade = section['grade']
                
                # Lookup dans la table 3D: (segment_dist, section_dist, grade) → time(s)
                time_seconds = interpolator([segment_distance_km, sect_dist_km, grade])[0]
                
                total_time += time_seconds
            
            return total_time
        
        def extract_features_from_sections(sections, segment_id=None, df=None):
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
            with open(repo_root / 'src' / 'models' / '250W_2900Ws_05_power_profile_time_lookup_table.pkl', 'rb') as f:
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
        
        def extract_features_for_dataframe(df, sections_dict):
            features_list = []
            segment_ids = []
            
            for idx, row in df.iterrows():
                segment_id = row['segment_id']
                
                if segment_id not in sections_dict:
                    print(f"Warning: No sections found for segment {segment_id}")
                    continue
                
                sections = sections_dict[segment_id]
                
                # Passer le DataFrame à la fonction
                features = extract_features_from_sections(sections, segment_id=segment_id, df=df)
                
                if features is not None:
                    features_list.append(features)
                    segment_ids.append(segment_id)
            
            features_df = pd.DataFrame(features_list, index=segment_ids)
            print(f"LOG: Extracted features for {len(features_df)} / {len(df)} segments")
            
            return features_df, segment_ids
        
        features_ride_df, segment_ids = extract_features_for_dataframe(ride_df, sections_dict_ride)
        return features_ride_df
    
    def cleaning(self, ride_df, sections_dict_ride, features_ride_df):
        # Too fast top 1 rider
        mask_too_fast_best_rider = (ride_df['best_time'] < ride_df['average_top_10_time'] / 2)
        ride_df.loc[mask_too_fast_best_rider, 'best_time'] = ride_df.loc[mask_too_fast_best_rider, 'average_top_10_time'].astype(int)
        
        # Too steep slopes
        mask_too_steep_slope = (features_ride_df['max_grade'] <= 30) & (features_ride_df['min_grade'] >= -30)
        features_ride_df = features_ride_df.loc[mask_too_steep_slope]
        sections_dict_ride = {k: v for k, v in sections_dict_ride.items() if k in ride_df.index}
        return ride_df, sections_dict_ride, features_ride_df
    
    def load_medium_confidence_T1_segments(self, df):
        T1_MC_active_learning_ride = pd.read_csv(repo_root / 'data' / 'processed' / 'T1_active_learning_threshold_5_ride.csv')
        T1_road_naming_ride = pd.read_csv(repo_root / 'data' / 'processed' / 'T1_road_naming_ride.csv')
        segments_manually_labeled = pd.read_csv(repo_root / 'data' / 'processed' / 'segments_manually_labeled.csv')
        T1_segments_manually_labeled_ride = segments_manually_labeled[(segments_manually_labeled['technicality'] == 1) & (segments_manually_labeled['segment_id'].isin(df[df['activity_type'] == 'Ride'].index))].copy()
        not_T1_segments_manually_labeled_ride = segments_manually_labeled[(segments_manually_labeled['technicality'] != 1) & (segments_manually_labeled['segment_id'].isin(df[df['activity_type'] == 'Ride'].index))].copy()

        T1_MC_ride = pd.concat([T1_MC_active_learning_ride, T1_road_naming_ride, T1_segments_manually_labeled_ride]).drop_duplicates().reset_index(drop=True)
        T1_MC_ride = T1_MC_ride.drop(T1_MC_ride[T1_MC_ride['segment_id'].isin(not_T1_segments_manually_labeled_ride['segment_id'])].index).reset_index(drop=True)

        T1_MC_HE_ride = df[(df['total_effort_count']>5000) & (df.index.isin(T1_MC_ride['segment_id']))].copy()
        return T1_MC_HE_ride, segments_manually_labeled
        
    def spliting(self, features_ride_df, T1_MC_ride):
        Train_ride, Test_ride = train_test_split(features_ride_df, test_size=0.2, random_state=42)
        Train_ride, Val_ride = train_test_split(Train_ride, test_size=0.25, random_state=42)
        Train_T1_MC_ride = Train_ride[Train_ride.index.isin(T1_MC_ride.index)].copy()
        Val_T1_MC_ride = Val_ride[Val_ride.index.isin(T1_MC_ride.index)].copy()
        Test_T1_MC_ride = Test_ride[Test_ride.index.isin(T1_MC_ride.index)].copy()
        return Train_ride, Val_ride, Test_ride, Train_T1_MC_ride, Val_T1_MC_ride, Test_T1_MC_ride
    
    def pipeline_model(self, Train_T1_MC_ride, ride_df):
        X_train_ride = Train_T1_MC_ride
        y_train_ride = ride_df.loc[Train_T1_MC_ride.index, 'best_time']

        features_to_exclude = ['total_distance_km', 'total_elevation_gain', 'total_elevation_loss','min_grade','max_grade', 'hardest_section_position','grade_variance']
        X_train_for_selection = X_train_ride.select_dtypes(include=[np.number]).drop(columns=features_to_exclude, errors='ignore')
        sfs = SequentialFeatureSelector(LinearRegression(), n_features_to_select='auto', direction='forward', scoring='neg_root_mean_squared_error', tol=0.1)
        sfs.fit(X_train_for_selection, y_train_ride)
        selected_features_1011 = X_train_for_selection.columns[sfs.get_support()].tolist()
        if 'total_distance_km' not in selected_features_1011:
            selected_features_1011.insert(0, 'total_distance_km')
        
        pipeline_1 = Pipeline([
            ('scaler', StandardScaler()),
            ('model', RidgeCV(cv=5))
            ])  

        pipeline_2 = Pipeline([
            ('scaler', StandardScaler()),
            ('model', RidgeCV(cv=5))
            ])
        pipeline_3 = Pipeline([
            ('scaler', StandardScaler()),
            ('model', RidgeCV(cv=5))
            ])  
        pipeline_4 = Pipeline([
            ('scaler', StandardScaler()),
            ('model', RidgeCV(cv=5))
            ])  

        model_router = ModelRouter4(pipeline_1, pipeline_2, pipeline_3, pipeline_4, threshold_1=1.0, threshold_2=2.0,threshold_3=4.0, segment_length_col='total_distance_km', drop_routing_col=True)
        model_router.fit(X_train_ride, y_train_ride)
        return model_router

    def save_model(self, model, file_path):
        with open(file_path, 'wb') as f:
            pickle.dump(model, f)
        print(f"LOG: Model saved to {file_path}")
            
    def load_model(self, file_path):
        with open(file_path, 'rb') as f:
            model = pickle.load(f)
        print(f"LOG: Model loaded from {file_path}")
        return model
    
    def predict(self, features_df):
        model = self.load_model(repo_root / 'src' / 'models' / 'T1_time_estimator_ride_model.pkl')
        X = features_df.drop(columns=['segment_id'])
        y_pred = model.predict(X)
        return y_pred
    
    def main(self, file_path):
        ride_df, sections_dict_ride = self.load_segment(file_path)
        features_ride_df = self.extract_features(ride_df, sections_dict_ride)  # ← AVANT set_index
        ride_df = ride_df.set_index('segment_id')  # ← APRÈS extract_features
        ride_df, sections_dict_ride, features_ride_df = self.cleaning(ride_df, sections_dict_ride, features_ride_df)
        print(f"LOG: Cleaning done")
        T1_MC_ride, segments_manually_labeled = self.load_medium_confidence_T1_segments(ride_df)
        print(f"LOG: Loaded medium confidence T1 segments")
        Train_ride, Val_ride, Test_ride, Train_T1_MC_ride, Val_T1_MC_ride, Test_T1_MC_ride = self.spliting(features_ride_df, T1_MC_ride)
        model = self.pipeline_model(Train_T1_MC_ride, ride_df)
        self.save_model(model, repo_root / 'src' / 'models' / 'T1_time_estimator_ride_model.pkl')
        
        return None
    
class ModelRouter4(BaseEstimator, RegressorMixin):
    def __init__(self, pipeline_1, pipeline_2, pipeline_3, pipeline_4,
                threshold_1=1.0, threshold_2=2.0, threshold_3=5.0, 
                segment_length_col='segment_length',
                drop_routing_col=True):  # ← NOUVEAU paramètre
        self.pipeline_1 = pipeline_1
        self.pipeline_2 = pipeline_2
        self.pipeline_3 = pipeline_3
        self.pipeline_4 = pipeline_4
        self.threshold_1 = threshold_1
        self.threshold_2 = threshold_2
        self.threshold_3 = threshold_3
        self.segment_length_col = segment_length_col
        self.drop_routing_col = drop_routing_col  # ← NOUVEAU

    def _prepare_features(self, X):
        """Enlève la colonne de routing des features si demandé"""
        if self.drop_routing_col and self.segment_length_col in X.columns:
            return X.drop(columns=[self.segment_length_col])
        return X

    def fit(self, X, y):
        # Calculer les masques AVANT de retirer la colonne
        self.mask_1 = X[self.segment_length_col] <= self.threshold_1
        self.mask_2 = (X[self.segment_length_col] > self.threshold_1) & \
                    (X[self.segment_length_col] <= self.threshold_2)
        self.mask_3 = (X[self.segment_length_col] > self.threshold_2) & \
                    (X[self.segment_length_col] <= self.threshold_3)
        self.mask_4 = X[self.segment_length_col] > self.threshold_3

        # Préparer les features SANS la colonne de routing
        X_features = self._prepare_features(X)

        # Entraîner chaque pipeline sur son subset
        if np.any(self.mask_1):
            self.pipeline_1.fit(X_features[self.mask_1], y[self.mask_1])
        if np.any(self.mask_2):
            self.pipeline_2.fit(X_features[self.mask_2], y[self.mask_2])
        if np.any(self.mask_3):
            self.pipeline_3.fit(X_features[self.mask_3], y[self.mask_3])
        if np.any(self.mask_4):
            self.pipeline_4.fit(X_features[self.mask_4], y[self.mask_4])
        
        return self

    def predict(self, X):
        # Calculer les masques AVANT de retirer la colonne
        mask_1 = X[self.segment_length_col] <= self.threshold_1
        mask_2 = (X[self.segment_length_col] > self.threshold_1) & \
                (X[self.segment_length_col] <= self.threshold_2)
        mask_3 = (X[self.segment_length_col] > self.threshold_2) & \
                (X[self.segment_length_col] <= self.threshold_3)
        mask_4 = X[self.segment_length_col] > self.threshold_3

        # Préparer les features SANS la colonne de routing
        X_features = self._prepare_features(X)

        y_pred = np.zeros(len(X))
        if np.any(mask_1):
            y_pred[mask_1] = self.pipeline_1.predict(X_features[mask_1])
        if np.any(mask_2):
            y_pred[mask_2] = self.pipeline_2.predict(X_features[mask_2])
        if np.any(mask_3):
            y_pred[mask_3] = self.pipeline_3.predict(X_features[mask_3])
        if np.any(mask_4):
            y_pred[mask_4] = self.pipeline_4.predict(X_features[mask_4])
        
        return y_pred

if __name__ == "__main__":
    file_path = repo_root / 'data' / 'processed' / 'reunion_segments_cleaned.parquet'
    T1_estimator = T1TimeEstimator()
    T1_estimator.main(file_path)