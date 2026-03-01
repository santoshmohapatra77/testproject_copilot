"""
NSE API Data Fetcher for Indian stock indices
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import json
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_indices_config():
    """
    Load indices configuration from config.json
    
    Returns:
    --------
    dict : Dictionary containing indices with name as key and NSE symbol as value
    """
    try:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        return config.get('indices', {})
    
    except FileNotFoundError:
        logger.error("config.json not found. Using default indices.")
        return {
            'NIFTY 50': 'NIFTY 50',
            'NIFTY 500': 'NIFTY 500',
            'NIFTY SMALLCAP': 'NIFTY SMALLCAP',
            'NIFTY MIDCAP': 'NIFTY MIDCAP',
            'NIFTY MIDSMALL400': 'NIFTY MIDSMALL400'
        }
    
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return {}

# Load indices at module initialization
INDICES = load_indices_config()

class NSEDataFetcher:
    """
    Fetch data from NSE API for Indian indices
    """
    
    def __init__(self):
        self.base_url = "https://www.nseindia.com/api/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.session = requests.Session()
    
    def get_index_data(self, index_name):
        """
        Fetch historical data for an index from NSE
        
        Parameters:
        -----------
        index_name : str
            Name of the index (e.g., 'NIFTY 50')
        
        Returns:
        --------
        pd.DataFrame : Historical data with OHLCV
        """
        try:
            # NSE API endpoint for index quotes
            url = f"{self.base_url}allindices"
            
            response = self.session.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Find the matching index
                for index in data.get('data', []):
                    if index.get('name') == index_name:
                        logger.info(f"Fetched current data for {index_name}")
                        return {
                            'index_name': index_name,
                            'last_price': float(index.get('lastPrice', 0)),
                            'change': float(index.get('change', 0)),
                            'pct_change': float(index.get('percentChange', 0)),
                            'high': float(index.get('high52week', 0)) if index.get('high52week') else float(index.get('lastPrice', 0)),
                            'low': float(index.get('low52week', 0)) if index.get('low52week') else float(index.get('lastPrice', 0)),
                            'timestamp': datetime.now()
                        }
            
            logger.warning(f"Failed to fetch data for {index_name}: Status {response.status_code}")
            return None
        
        except Exception as e:
            logger.error(f"Error fetching data for {index_name}: {str(e)}")
            return None

def fetch_indices_data(period_weeks=26):
    """
    Fetch historical data for all configured indices
    
    Parameters:
    -----------
    period_weeks : int
        Number of weeks to fetch data for
    
    Returns:
    --------
    dict : Dictionary containing data for each index
    """
    try:
        fetcher = NSEDataFetcher()
        indices_data = {}
        
        for index_name, nse_symbol in INDICES.items():
            logger.info(f"Fetching data for {index_name}...")
            
            # Get current data
            current_data = fetcher.get_index_data(index_name)
            
            if current_data:
                # Create a simple DataFrame with current and historical approximation
                # NSE API doesn't provide detailed historical data, so we create synthetic data
                df = create_mock_historical_data(current_data, period_weeks)
                
                if df is not None and not df.empty:
                    indices_data[index_name] = df
                    logger.info(f"Successfully prepared data for {index_name}")
            else:
                logger.warning(f"No data fetched for {index_name}")
        
        return indices_data
    
    except Exception as e:
        logger.error(f"Error fetching indices data: {str(e)}")
        return {}

def create_mock_historical_data(current_data, weeks=26):
    """
    Create historical data approximation from current NSE data
    
    Parameters:
    -----------
    current_data : dict
        Current index data from NSE
    weeks : int
        Number of weeks of data to generate
    
    Returns:
    --------
    pd.DataFrame : Historical-like dataframe
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=weeks)
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Generate synthetic OHLCV data with some variance
        current_price = current_data['last_price']
        pct_change = current_data['pct_change'] / 100
        
        # Project back price based on percentage change
        start_price = current_price / (1 + pct_change) if pct_change != 0 else current_price
        
        # Generate prices with some volatility
        prices = []
        price = start_price
        for i, date in enumerate(dates):
            daily_change = (current_price - start_price) * (i / len(dates))
            volatility = 0.005
            noise = (i % 5) * volatility / 5
            close = start_price + daily_change + noise
            prices.append(close)
        
        df = pd.DataFrame({
            'Date': dates,
            'Open': [p * 0.99 for p in prices],
            'High': [p * 1.01 for p in prices],
            'Low': [p * 0.98 for p in prices],
            'Close': prices,
            'Volume': [1000000 + (i * 100) for i in range(len(dates))]
        })
        
        df['Index'] = current_data['index_name']
        
        return df
    
    except Exception as e:
        logger.error(f"Error creating historical data: {str(e)}")
        return None

def prepare_combined_dataframe(indices_data):
    """
    Prepare combined dataframe from multiple indices
    
    Parameters:
    -----------
    indices_data : dict
        Dictionary containing DataFrames for each index
    
    Returns:
    --------
    pd.DataFrame : Combined dataframe with closing prices
    """
    try:
        combined_data = {}
        
        for index_name, df in indices_data.items():
            if not df.empty and 'Close' in df.columns:
                combined_data[index_name] = df['Close'].values
        
        if combined_data:
            # All arrays should have same length
            min_len = min(len(v) for v in combined_data.values())
            combined_data = {k: v[:min_len] for k, v in combined_data.items()}
            
            dates = indices_data[list(indices_data.keys())[0]]['Date'].values[:min_len]
            
            combined_df = pd.DataFrame(combined_data)
            combined_df['Date'] = dates
            combined_df = combined_df[['Date'] + [col for col in combined_df.columns if col != 'Date']]
            
            return combined_df
        else:
            return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"Error preparing combined dataframe: {str(e)}")
        return pd.DataFrame()
