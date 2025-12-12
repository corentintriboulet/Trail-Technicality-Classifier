import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split, cross_val_score
import matplotlib.pyplot as plt
import seaborn as sns


class TheoreticalBestTimeEstimator:
    """
    Stage 1: Estimates theoretical best time accounting for sampling bias
    Uses terrain features + effort count to predict true best time
    """
    
    def __init__(self):
        self.model = None
    
    def correction_function(self, effort_counts):
        """
        Correction function to adjust predicted times based on effort counts
        """
        return 1 + 2/3*np.exp(-effort_counts/1000)
        
    def estimated_th_best_time(self, df):
        """
        Estimate theoretical best time for given features X
        """
        best_time_column = 'best_time'
        effort_count_column = 'total_effort_count'
        df = df.copy()
        
        # create a new df vertically (1 column x n rows), stacking theorical best times for each segment, n is number of segments in df
        df_new = pd.DataFrame(columns=['theoretical_best_time'])
        
        for index, row in df.iterrows():
            correction = self.correction_function(row[effort_count_column])
            theorical_best_time = row[best_time_column] * correction
            df_new = pd.concat([df_new, pd.DataFrame({'theoretical_best_time': [theorical_best_time]})], ignore_index=False)
            df_new.index.values[-1] = index  # set the index to be the same as the original df
        

        return df_new