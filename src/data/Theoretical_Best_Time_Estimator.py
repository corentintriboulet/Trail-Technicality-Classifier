"""
Corrects observed best times based on segment popularity (effort count).
Segments with fewer efforts have higher sampling bias and need larger corrections.
Uses exponential decay function: correction = 1 + (2/3) * exp(-efforts/1000)
"""

import numpy as np
import pandas as pd


class TheoreticalBestTimeEstimator:
    """Estimates theoretical best time correcting for sampling bias"""
    
    def __init__(self):
        self.model = None
    
    def correction_function(self, effort_counts):
        """
        Calculate correction factor based on effort count
        
        Higher effort counts → lower correction (more reliable best time)
        Lower effort counts → higher correction (sampling bias)
        
        Args:
            effort_counts (float or array): Total number of efforts on segment
        
        Returns:
            float or array: Correction multiplier (>= 1.0)
        """
        return 1 + (2/3) * np.exp(-effort_counts / 1000)
        
    def estimated_th_best_time(self, df):
        """
        Estimate theoretical best time for all segments in DataFrame
        
        Args:
            df (DataFrame): Segment data with 'best_time' and 'total_effort_count' columns
        
        Returns:
            DataFrame: Single column DataFrame with 'theoretical_best_time'
        """
        best_time_column = 'best_time'
        effort_count_column = 'total_effort_count'
        
        df = df.copy()
        
        # Apply correction to each segment
        corrections = self.correction_function(df[effort_count_column])
        theoretical_best_times = df[best_time_column] * corrections
        
        # Create result DataFrame with matching index
        df_new = pd.DataFrame({
            'theoretical_best_time': theoretical_best_times
        }, index=df.index)
        
        return df_new