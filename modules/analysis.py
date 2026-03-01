"""
Analysis module for stock indices
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class IndicesAnalyzer:
    """
    Class to perform technical and statistical analysis on indices
    """
    
    def __init__(self, indices_data):
        """
        Initialize analyzer with indices data
        
        Parameters:
        -----------
        indices_data : dict
            Dictionary containing DataFrames for each index
        """
        self.indices_data = indices_data
        self.analysis_results = {}
    
    def calculate_metrics(self):
        """
        Calculate key metrics for each index
        
        Returns:
        --------
        pd.DataFrame : Analysis results with metrics
        """
        try:
            metrics_list = []
            
            for index_name, df in self.indices_data.items():
                if df.empty:
                    continue
                
                close_prices = df['Close'].values
                
                # Calculate metrics
                current_price = close_prices[-1]
                start_price = close_prices[0]
                change = current_price - start_price
                pct_change = (change / start_price) * 100
                
                high_price = df['High'].max()
                low_price = df['Low'].min()
                
                # Calculate volatility (standard deviation of daily returns)
                daily_returns = df['Close'].pct_change().dropna()
                volatility = daily_returns.std() * np.sqrt(252) * 100  # Annualized
                
                # Calculate average volume
                avg_volume = df['Volume'].mean() if 'Volume' in df.columns else 0
                
                # Moving averages
                ma_50 = df['Close'].rolling(window=50).mean().iloc[-1]
                ma_20 = df['Close'].rolling(window=20).mean().iloc[-1]
                
                metrics_list.append({
                    'Index': index_name,
                    'Current Price': round(current_price, 2),
                    'Start Price': round(start_price, 2),
                    'Change (%)': round(pct_change, 2),
                    'Absolute Change': round(change, 2),
                    '52W High': round(high_price, 2),
                    '52W Low': round(low_price, 2),
                    'Volatility (%)': round(volatility, 2),
                    'MA-20': round(ma_20, 2),
                    'MA-50': round(ma_50, 2),
                    'Avg Volume': f"{avg_volume:,.0f}"
                })
            
            self.analysis_results = pd.DataFrame(metrics_list)
            return self.analysis_results
        
        except Exception as e:
            logger.error(f"Error calculating metrics: {str(e)}")
            return pd.DataFrame()
    
    def get_performance_ranking(self):
        """
        Rank indices by performance
        
        Returns:
        --------
        pd.DataFrame : Ranked indices by percentage change
        """
        try:
            if self.analysis_results.empty:
                self.calculate_metrics()
            
            ranking = self.analysis_results[['Index', 'Change (%)', 'Volatility (%)']].copy()
            ranking['Rank'] = ranking['Change (%)'].rank(ascending=False).astype(int)
            ranking = ranking.sort_values('Rank')
            
            return ranking
        
        except Exception as e:
            logger.error(f"Error getting performance ranking: {str(e)}")
            return pd.DataFrame()
    
    def get_indices_strength(self):
        """
        Calculate relative strength of indices
        
        Returns:
        --------
        pd.DataFrame : Strength metrics
        """
        try:
            if self.analysis_results.empty:
                self.calculate_metrics()
            
            results = self.analysis_results[['Index', 'Change (%)']].copy()
            
            # Normalize to 0-100 scale for strength
            min_change = results['Change (%)'].min()
            max_change = results['Change (%)'].max()
            
            if max_change > min_change:
                results['Strength Score'] = ((results['Change (%)'] - min_change) / 
                                           (max_change - min_change) * 100).round(2)
            else:
                results['Strength Score'] = 50
            
            return results.sort_values('Strength Score', ascending=False)
        
        except Exception as e:
            logger.error(f"Error calculating strength: {str(e)}")
            return pd.DataFrame()
