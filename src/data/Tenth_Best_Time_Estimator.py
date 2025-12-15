"""
Tenth_Best_Time_Estimator.py

Predicts 10th place time on segment leaderboards using ML model

Uses features: average_top_10_time, best_time, log(effort_count), log(athlete_count)
Model is pre-trained and loaded from pickle file.
"""

import joblib


class TenthBestTimeEstimator:
    """Predicts 10th best time using pre-trained model"""
    
    def __init__(self, model_path):
        """
        Initialize estimator with pre-trained model
        
        Args:
            model_path (str or Path): Path to saved model pickle file
        """
        self.model = joblib.load(model_path)

    def add_tenth_best_time(self, X):
        """
        Predict tenth best time and add to DataFrame
        
        Args:
            X (DataFrame): Segment data with required columns:
                - average_top_10_time
                - best_time
                - total_effort_count
                - total_athlete_count
        
        Returns:
            DataFrame: Input DataFrame with added 'tenth_best_time' column
        
        Raises:
            TypeError: If input is not a pandas DataFrame
        """
        # Validate input type
        if not isinstance(X, pd.DataFrame):
            raise TypeError("Input X must be a pandas DataFrame")
        
        # Deep copy to avoid modifying original
        X_copy = X.copy(deep=True)

        # Compute log-transformed features
        X_copy['log_efforts'] = np.log(X_copy['total_effort_count'] + 1)
        X_copy['log_athletes'] = np.log(X_copy['total_athlete_count'] + 1)

        # Select features expected by model
        model_features = ['average_top_10_time', 'best_time', 'log_efforts', 'log_athletes']
        X_model = X_copy[model_features]

        # Generate predictions
        predictions = self.model.predict(X_model)
        
        # Ensure 1D array
        if len(predictions.shape) > 1:
            predictions = predictions.flatten()

        # Add predictions to DataFrame
        X_copy['tenth_best_time'] = predictions
        
        # Verification
        assert isinstance(X_copy, pd.DataFrame), "Output is not a DataFrame!"
        assert 'tenth_best_time' in X_copy.columns, "tenth_best_time column missing!"
        
        return X_copy