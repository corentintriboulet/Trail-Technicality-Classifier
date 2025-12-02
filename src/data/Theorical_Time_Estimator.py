import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split, cross_val_score
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import matplotlib.pyplot as plt
import seaborn as sns


class TheoreticalTimeEstimator:
    """
    Stage 1: Estimates theoretical best time accounting for sampling bias
    Uses terrain features + effort count to predict true best time
    """
    
    def __init__(self, max_sections=50):
        self.model = None
        self.scaler_sections = StandardScaler()
        self.scaler_global = StandardScaler()
        self.max_sections = max_sections
        
    def _prepare_section_features(self, sections):
        """Extract features from each section"""
        features = []
        for s in sections:
            features.append([
                s['distance'],
                s['elevation_gain'],
                s['elevation_loss'],
                s['grade'],
                s['max_grade'],
                s['min_grade'],
                s['grade_variance'],
                1 if s['type'] == 'climb' else 0,  # is_climb
            ])
        return np.array(features)
    
    def _pad_sections(self, section_features):
        """Pad sections to max_sections length"""
        n_sections, n_features = section_features.shape
        
        if n_sections >= self.max_sections:
            return section_features[:self.max_sections]
        
        padded = np.zeros((self.max_sections, n_features))
        padded[:n_sections] = section_features
        return padded
    
    def _extract_global_features(self, sections):
        """Extract global segment features"""
        total_distance = sum(s['distance'] for s in sections)
        total_elevation_gain = sum(s['elevation_gain'] for s in sections)
        
        # Fatigue-weighted features (later sections weighted more)
        weighted_grade = 0
        for i, s in enumerate(sections):
            weight = 1 + (i / len(sections)) * 0.5  # 1.0 to 1.5x
            weighted_grade += s['grade'] * weight * s['distance']
        weighted_grade /= total_distance
        
        # Climb distribution
        early_climb = sum(s['elevation_gain'] for s in sections[:len(sections)//3])
        late_climb = sum(s['elevation_gain'] for s in sections[-(len(sections)//3):])
        
        return np.array([
            total_distance,
            total_elevation_gain,
            np.mean([s['grade'] for s in sections]),
            max(s['max_grade'] for s in sections),
            weighted_grade,
            early_climb / (total_elevation_gain + 1e-6),
            late_climb / (total_elevation_gain + 1e-6),
            len(sections),
        ])
    
    def _build_model(self, n_section_features, n_global_features):
        """Build the theoretical time prediction model"""
        # Section sequence input
        section_input = layers.Input(shape=(self.max_sections, n_section_features))
        mask = layers.Masking(mask_value=0.0)(section_input)
        lstm = layers.Bidirectional(layers.LSTM(32, return_sequences=False, dropout=0.3))(mask)
        
        # Global features
        global_input = layers.Input(shape=(n_global_features,))
        
        # Effort/sampling features
        sampling_input = layers.Input(shape=(3,))  # log(effort), log(athletes), best_time
        sampling_dense = layers.Dense(16, activation='relu')(sampling_input)
        
        # Combine
        combined = layers.Concatenate()([lstm, global_input, sampling_dense])
        dense1 = layers.Dense(64, activation='relu')(combined)
        dense1 = layers.Dropout(0.3)(dense1)
        dense2 = layers.Dense(32, activation='relu')(dense1)
        output = layers.Dense(1)(dense2)
        
        model = keras.Model(
            inputs=[section_input, global_input, sampling_input],
            outputs=output
        )
        model.compile(optimizer='adam', loss='mse', metrics=['mae'])
        return model
    
    def fit(self, segments_df, sections_by_segment, validation_split=0.2):
        """Train the model on high-effort segments"""
        # Filter to high-effort segments (reliable ground truth)
        high_effort = segments_df[segments_df['total_effort_count'] > 500].copy()
        print(f"Training on {len(high_effort)} high-effort segments")
        
        # Prepare features
        X_sections = []
        X_global = []
        X_sampling = []
        y = []
        
        for _, row in high_effort.iterrows():
            sections = sections_by_segment[row['id']]
            
            section_features = self._prepare_section_features(sections)
            X_sections.append(self._pad_sections(section_features))
            X_global.append(self._extract_global_features(sections))
            X_sampling.append([
                np.log1p(row['total_effort_count']),
                np.log1p(row['total_athlete_count']),
                row['best_time']
            ])
            y.append(row['best_time'])
        
        X_sections = np.array(X_sections)
        X_global = np.array(X_global)
        X_sampling = np.array(X_sampling)
        y = np.array(y)
        
        # Normalize (don't normalize sections - keep raw for masking)
        X_global = self.scaler_global.fit_transform(X_global)
        
        # Build and train
        n_section_features = X_sections.shape[2]
        n_global_features = X_global.shape[1]
        
        self.model = self._build_model(n_section_features, n_global_features)
        
        history = self.model.fit(
            [X_sections, X_global, X_sampling], y,
            validation_split=validation_split,
            epochs=100,
            batch_size=32,
            callbacks=[
                keras.callbacks.EarlyStopping(patience=15, restore_best_weights=True),
                keras.callbacks.ReduceLROnPlateau(patience=5, factor=0.5)
            ],
            verbose=1
        )
        
        return history
    
    def predict(self, segments_df, sections_by_segment):
        """Predict theoretical best time for all segments"""
        predictions = []
        
        for _, row in segments_df.iterrows():
            sections = sections_by_segment[row['id']]
            
            section_features = self._prepare_section_features(sections)
            X_section = self._pad_sections(section_features).reshape(1, self.max_sections, -1)
            X_global = self.scaler_global.transform(
                self._extract_global_features(sections).reshape(1, -1)
            )
            X_sampling = np.array([[
                np.log1p(row['total_effort_count']),
                np.log1p(row['total_athlete_count']),
                row['best_time']
            ]])
            
            pred = self.model.predict([X_section, X_global, X_sampling], verbose=0)[0][0]
            predictions.append(pred)
        
        return np.array(predictions)