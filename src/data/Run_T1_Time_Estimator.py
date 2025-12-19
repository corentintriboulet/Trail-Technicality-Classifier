import os
import sys
from pathlib import Path
current_script_path = Path(__file__).resolve()
repo_root = current_script_path.parents[2]
sys.path.append(str(repo_root))


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
from scipy.optimize import fsolve
from scipy.interpolate import interp1d
import pickle

class runT1TimeEstimator():
    """
    Stage 2: Estimates T1 time using terrain features
    Uses terrain features to predict T1 time
    """
    
    def __init__(self):
        return None
    

    def spliting(self, features_run_df, T1_MC_run):
        Train_run, Test_run = train_test_split(features_run_df, test_size=0.2, random_state=42)
        Train_run, Val_run = train_test_split(Train_run, test_size=0.25, random_state=42)
        Train_T1_MC_run = Train_run[Train_run.index.isin(T1_MC_run.index)].copy()
        Val_T1_MC_run = Val_run[Val_run.index.isin(T1_MC_run.index)].copy()
        Test_T1_MC_run = Test_run[Test_run.index.isin(T1_MC_run.index)].copy()
        return Train_run, Val_run, Test_run, Train_T1_MC_run, Val_T1_MC_run, Test_T1_MC_run
    
    def high_effort_filter(self, df, threshold=5000):
        df_high_effort = df[df['total_efforts_count'] >= threshold].copy()
        return df_high_effort

    def pipeline_model(self, Train_T1_MC_run, run_df):
        X_train_run = Train_T1_MC_run
        y_train_run = run_df.loc[Train_T1_MC_run.index, 'best_time']

        features_to_exclude = ['total_distance_km', 'total_elevation_gain', 'total_elevation_loss','min_grade','max_grade', 'hardest_section_position','grade_variance']
        X_train_for_selection = X_train_run.select_dtypes(include=[np.number]).drop(columns=features_to_exclude, errors='ignore')
        sfs = SequentialFeatureSelector(LinearRegression(), n_features_to_select='auto', direction='forward', scoring='neg_root_mean_squared_error', tol=0.1)
        sfs.fit(X_train_for_selection, y_train_run)
        selected_features = X_train_for_selection.columns[sfs.get_support()].tolist()
        if 'total_distance_km' not in selected_features:
            selected_features.insert(0, 'total_distance_km')
        
        pipeline_1 = Pipeline([
            ('scaler', StandardScaler()),
            ('model', RidgeCV(cv=2))
            ])  

        pipeline_2 = Pipeline([
            ('scaler', StandardScaler()),
            ('model', RidgeCV(cv=2))
            ])
        pipeline_3 = Pipeline([
            ('scaler', StandardScaler()),
            ('model', RidgeCV(cv=2))
            ])  
        pipeline_4 = Pipeline([
            ('scaler', StandardScaler()),
            ('model', RidgeCV(cv=2))
            ])  

        model_router = ModelRouter4(pipeline_1, pipeline_2, pipeline_3, pipeline_4, threshold_1=1.0, threshold_2=2.0,threshold_3=4.0, segment_length_col='total_distance_km', drop_routing_col=True)
        model_router.fit(X_train_run, y_train_run)
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
    
    def plotting_MAE_bins(y_train, y_train_pred, y_val, y_val_pred, y_test, y_test_pred):
        bins = [0, 60, 3*60, 5*60, 15*60, 3600, np.inf]
        labels = ['<1min', '1-3min','3-5min', '5-15min', '15-60min', '>60min']

        train_df = pd.DataFrame({
            'actual': y_train,
            'pred': y_train_pred
        })
        train_df['bin'] = pd.cut(train_df['actual'], bins=bins, labels=labels)

        val_df = pd.DataFrame({
            'actual': y_val,
            'pred': y_val_pred
        })
        val_df['bin'] = pd.cut(val_df['actual'], bins=bins, labels=labels)

        test_df = pd.DataFrame({
            'actual': y_test,
            'pred': y_test_pred
        })
        test_df['bin'] = pd.cut(test_df['actual'], bins=bins, labels=labels)

        results = []
        for bin_label in labels:
            train_mask = train_df['bin'] == bin_label
            val_mask = val_df['bin'] == bin_label
            test_mask = test_df['bin'] == bin_label
            
            if train_mask.sum() > 0 and val_mask.sum() > 0 and test_mask.sum() > 0:
                train_mae = np.mean(np.abs(train_df.loc[train_mask, 'actual'] - train_df.loc[train_mask, 'pred']))
                val_mae = np.mean(np.abs(val_df.loc[val_mask, 'actual'] - val_df.loc[val_mask, 'pred']))
                test_mae = np.mean(np.abs(test_df.loc[test_mask, 'actual'] - test_df.loc[test_mask, 'pred']))
                
                train_n = train_mask.sum()
                val_n = val_mask.sum()
                test_n = test_mask.sum()
                
                results.append({
                    'Time': bin_label,
                    'Train N': train_n,
                    'Train MAE': f"{train_mae:.0f}s",
                    'Val N': val_n,
                    'Val MAE': f"{val_mae:.0f}s",
                    'Test N': test_n,
                    'Test MAE': f"{test_mae:.0f}s"
                })

        # Créer et afficher le tableau
        results_df = pd.DataFrame(results)

        print("\n" + "="*70)
        print("Bin'S MAE Results")
        print("="*70)
        print(results_df.to_string(index=False))
        print("="*70)

        # Métriques globales
        print(f"\nGLOBAL - Train RMSE: {root_mean_squared_error(y_train, y_train_pred):.2f}")
        print(f"GLOBAL - Train MAE: {mean_absolute_error(y_train, y_train_pred):.1f}s")
        print(f"GLOBAL - Validation RMSE: {root_mean_squared_error(y_val, y_val_pred):.2f}")
        print(f"GLOBAL - Validation MAE: {mean_absolute_error(y_val, y_val_pred):.1f}s")
        return None
    
    def predict(self, features_df):
        model = self.load_model(repo_root / 'src' / 'models' / 'T1_time_estimator_run_model.pkl')
        X = features_df.drop(columns=['segment_id'])
        y_pred = model.predict(X)
        return y_pred
    
    def get_predictions(self, T1_MC_run, features_run_df):
        model_path = repo_root / 'src' / 'models' / 'T1_time_estimator_run_model.pkl'
        if os.path.exists(model_path):
            print("LOG: Loading existing T1 time estimator model for run...")
            model = self.load_model(model_path)
            
            
        else:
            print("LOG: Training new T1 time estimator model for run...")
            Train_run, Val_run, Test_run, Train_T1_MC_run, Val_T1_MC_run, Test_T1_MC_run = self.spliting(features_run_df, T1_MC_run)
            Train_T1_MC_run = self.high_effort_filter(Train_T1_MC_run, threshold=5000)
            model = self.pipeline_model(Train_T1_MC_run, features_run_df)
            self.save_model(model, model_path)
            self.plotting_MAE_bins(
                y_train=features_run_df.loc[Train_T1_MC_run.index, 'best_time'],
                y_train_pred=model.predict(Train_T1_MC_run),
                y_val=features_run_df.loc[Val_T1_MC_run.index, 'best_time'],
                y_val_pred=model.predict(Val_T1_MC_run),
                y_test=features_run_df.loc[Test_T1_MC_run.index, 'best_time'],
                y_test_pred=model.predict(Test_T1_MC_run)
            )
        
        # Make predictions on a COPY to avoid modifying the original
        result_df = features_run_df.copy()
        y_all_pred = model.predict(features_run_df)  # Use original for prediction
        result_df['predicted_T1_time'] = y_all_pred  # Add to copy
        return result_df
    
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
