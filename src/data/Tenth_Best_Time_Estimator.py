import pandas as pd
import numpy as np
import joblib

class TenthBestTimeEstimator: 
    def __init__(self, model_path): 
        self.model = joblib.load(model_path) 

    def add_tenth_best_time(self, X): 
        """
        Predict the tenth best time for given segments and return a DataFrame.
        Ensures the return is a proper DataFrame, not a NumPy array.
        """
        # Ensure input is a DataFrame
        if not isinstance(X, pd.DataFrame):
            raise TypeError("Input X must be a pandas DataFrame")
        
        # Store original index and columns
        original_index = X.index
        original_columns = X.columns.tolist()
        
        # Make a proper deep copy
        X_copy = X.copy(deep=True)

        # Compute features expected by the model
        X_copy['log_efforts'] = np.log(X_copy['total_effort_count'] + 1)
        X_copy['log_athletes'] = np.log(X_copy['total_athlete_count'] + 1)

        # Columns the model expects
        model_features = ['average_top_10_time', 'best_time', 'log_efforts', 'log_athletes']
        X_model = X_copy[model_features]

        # Predict (NumPy array)
        predictions = self.model.predict(X_model)
        
        # Ensure predictions is 1D
        if len(predictions.shape) > 1:
            predictions = predictions.flatten()

        # Add predictions to the DataFrame
        X_copy['tenth_best_time'] = predictions
        
        # Verification
        assert isinstance(X_copy, pd.DataFrame), "Output is not a DataFrame!"
        assert 'tenth_best_time' in X_copy.columns, "tenth_best_time column missing!"
        
        return X_copy