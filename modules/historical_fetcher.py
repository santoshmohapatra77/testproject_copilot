"""
Historical data fetcher from yfinance and SQLite storage
"""
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
import json
import os
from modules.database import (
    initialize_database, insert_index, bulk_insert_daily_data,
    get_index_data, get_all_indices, get_data_status, 
    clear_old_data, get_data_date_range, get_indices_with_data
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_indices_config():
    """
    Load indices configuration from config.json
    
    Returns:
    --------
    dict : Dictionary containing indices with name as key and ticker as value
    """
    try:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        return config.get('indices', {})
    
    except FileNotFoundError:
        logger.error("config.json not found. Using default indices.")
        return {
            'NIFTY 50': '^NSEI',
            'NIFTY 500': '^NIFTY500',
            'NIFTY SMALLCAP': '^NSMALLCAP',
            'NIFTY MIDCAP': '^NSMIDCAP',
            'NIFTY MIDSMALL400': '^NSMIDSMALL400'
        }
    
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return {}

# Load indices at module initialization
INDICES = load_indices_config()

def fetch_and_store_historical_data(period_weeks=26):
    """
    Fetch historical closing price data from yfinance and store in SQLite
    
    Parameters:
    -----------
    period_weeks : int
        Number of weeks of historical data to fetch
    
    Returns:
    --------
    dict : Dictionary containing fetched data for each index
    """
    try:
        # Initialize database
        initialize_database()
        
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=period_weeks)
        
        indices_data = {}
        
        for index_name, ticker in INDICES.items():
            logger.info(f"Downloading historical data for {index_name} ({ticker})...")
            
            try:
                # Download data from yfinance
                df = yf.download(
                    ticker,
                    start=start_date,
                    end=end_date,
                    progress=False,
                    auto_adjust=True
                )
                
                if df.empty:
                    logger.warning(f"No data downloaded for {index_name}")
                    continue
                
                # Reset index to get date as column
                if isinstance(df, pd.Series):
                    logger.warning(f"Received Series instead of DataFrame for {index_name}")
                    continue
                
                df = df.reset_index()
                
                # Handle multi-level columns from yfinance
                # Flatten columns if they are tuples
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
                
                # Lowercase column names for consistency
                df.columns = [str(col).lower() for col in df.columns]
                
                # Keep only date and close columns
                # Look for variations of column names
                date_col = None
                close_col = None
                
                for col in df.columns:
                    if 'date' in col:
                        date_col = col
                    elif 'close' in col:
                        close_col = col
                
                if date_col is None or close_col is None:
                    logger.warning(f"Missing Date or Close column for {index_name}. Available columns: {df.columns.tolist()}")
                    continue
                
                # Create new dataframe with date and close
                df = df[[date_col, close_col]].copy()
                df.columns = ['Date', 'Close']
                
                # Convert date to string format (YYYY-MM-DD)
                df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                
                # Store in database
                if bulk_insert_daily_data(index_name, df):
                    indices_data[index_name] = df
                    logger.info(f"Successfully stored {len(df)} closing prices for {index_name}")
                else:
                    logger.warning(f"Failed to store data for {index_name}")
            
            except Exception as e:
                logger.error(f"Error downloading/storing data for {index_name}: {str(e)}")
                continue
        
        # Clean old data (keep only 180 days)
        deleted = clear_old_data(days_to_keep=180)
        logger.info(f"Cleaned database: removed {deleted} old records")
        
        return indices_data
    
    except Exception as e:
        logger.error(f"Error in fetch_and_store_historical_data: {str(e)}")
        return {}

def get_indices_data_from_db(period_days=180):
    """
    Retrieve indices data from SQLite database
    
    Parameters:
    -----------
    period_days : int
        Number of days of data to retrieve (default 180 = 26 weeks)
    
    Returns:
    --------
    dict : Dictionary containing DataFrames for each index
    """
    try:
        indices_data = {}
        
        for index_name in get_all_indices():
            logger.info(f"Retrieving data for {index_name} from database...")
            
            df = get_index_data(index_name, days=period_days)
            
            if not df.empty:
                indices_data[index_name] = df
                logger.info(f"Retrieved {len(df)} records for {index_name}")
            else:
                logger.warning(f"No data found in database for {index_name}")
        
        return indices_data
    
    except Exception as e:
        logger.error(f"Error retrieving data from database: {str(e)}")
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
        dates = None
        
        for index_name, df in indices_data.items():
            if not df.empty and 'Close' in df.columns:
                combined_data[index_name] = df['Close'].values
                if dates is None:
                    dates = df['Date'].values
        
        if combined_data and dates is not None:
            # Create combined DataFrame
            combined_df = pd.DataFrame(combined_data)
            combined_df['Date'] = dates
            
            # Reorder columns to have Date first
            combined_df = combined_df[['Date'] + [col for col in combined_df.columns if col != 'Date']]
            
            logger.info(f"Created combined dataframe with {len(combined_df)} rows and {len(combined_data)} indices")
            return combined_df
        else:
            logger.warning("Unable to create combined dataframe")
            return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"Error preparing combined dataframe: {str(e)}")
        return pd.DataFrame()

def get_data_download_status():
    """
    Get status of data in local database
    
    Returns:
    --------
    dict : Status information
    """
    return get_data_status()

def sync_database(force_refresh=False):
    """
    Sync database with latest data
    
    Parameters:
    -----------
    force_refresh : bool
        Force re-download of all data
    
    Returns:
    --------
    bool : Success status
    """
    try:
        if force_refresh:
            logger.info("Force refreshing all data...")
        
        data = fetch_and_store_historical_data(period_weeks=26)
        
        if data:
            logger.info(f"Successfully synced {len(data)} indices")
            return True
        else:
            logger.warning("No data synced")
            return False
    
    except Exception as e:
        logger.error(f"Error syncing database: {str(e)}")
        return False

def get_data_date_range_info():
    """
    Get the date range of data collection
    
    Returns:
    --------
    dict : Start and end dates
    """
    return get_data_date_range()

def verify_download_status():
    """
    Verify which indices have been downloaded and stored
    
    Returns:
    --------
    dict : Status of each configured index
    """
    try:
        indices_with_data = get_indices_with_data()
        configured_indices = list(INDICES.keys())
        
        status = {
            'downloaded': [],
            'missing': [],
            'total_configured': len(configured_indices),
            'total_downloaded': len(indices_with_data)
        }
        
        for index_name in configured_indices:
            if index_name in indices_with_data:
                status['downloaded'].append(index_name)
            else:
                status['missing'].append(index_name)
        
        return status
    
    except Exception as e:
        logger.error(f"Error verifying download status: {str(e)}")
        return {'downloaded': [], 'missing': [], 'total_configured': 0, 'total_downloaded': 0}
