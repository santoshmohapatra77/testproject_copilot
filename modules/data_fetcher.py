"""
Data fetcher module for Indian stock indices
"""
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Major Indian Stock Indices
INDICES = {
    'NIFTY 50': '^NSEI',
    'NIFTY IT': '^NSMIT',
    'NIFTY BANK': '^NSEBANK',
    'NIFTY PHARMA': '^NSEMDCP50',
    'NIFTY AUTO': '^CNXIT',
    'SENSEX': '^BSESN',
}

def fetch_indices_data(period_weeks=26):
    """
    Fetch historical data for Indian stock indices
    
    Parameters:
    -----------
    period_weeks : int
        Number of weeks to fetch data for (default: 26 weeks = 6 months)
    
    Returns:
    --------
    dict : Dictionary containing DataFrames for each index
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=period_weeks)
        
        indices_data = {}
        
        for index_name, ticker in INDICES.items():
            logger.info(f"Fetching data for {index_name}...")
            
            df = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                progress=False
            )
            
            if not df.empty:
                df['Index'] = index_name
                df['Ticker'] = ticker
                indices_data[index_name] = df
                logger.info(f"Successfully fetched {len(df)} rows for {index_name}")
            else:
                logger.warning(f"No data fetched for {index_name}")
        
        return indices_data
    
    except Exception as e:
        logger.error(f"Error fetching indices data: {str(e)}")
        return {}

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
                combined_data[index_name] = df['Close']
        
        if combined_data:
            combined_df = pd.DataFrame(combined_data)
            combined_df.index.name = 'Date'
            return combined_df.reset_index()
        else:
            return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"Error preparing combined dataframe: {str(e)}")
        return pd.DataFrame()
