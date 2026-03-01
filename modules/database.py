"""
SQLite Database module for indices data storage and retrieval
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_NAME = "indices_data.db"

def get_db_connection():
    """
    Get SQLite database connection
    
    Returns:
    --------
    sqlite3.Connection : Database connection
    """
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        return None

def initialize_database():
    """
    Initialize SQLite database with required tables
    """
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Create indices table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS indices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_name TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create daily_data table (closing price only)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_name TEXT NOT NULL,
                date DATE NOT NULL,
                close REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (index_name) REFERENCES indices(index_name),
                UNIQUE(index_name, date)
            )
        ''')
        
        # Create index for faster queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_index_date 
            ON daily_data(index_name, date DESC)
        ''')
        
        conn.commit()
        logger.info("Database initialized successfully")
        return True
    
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False
    
    finally:
        if conn:
            conn.close()

def insert_index(index_name):
    """
    Insert or update an index record
    
    Parameters:
    -----------
    index_name : str
        Name of the index
    
    Returns:
    --------
    bool : Success status
    """
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO indices (index_name) 
            VALUES (?)
        ''', (index_name,))
        
        conn.commit()
        logger.info(f"Index '{index_name}' inserted/verified in database")
        return True
    
    except Exception as e:
        logger.error(f"Error inserting index: {str(e)}")
        return False
    
    finally:
        if conn:
            conn.close()

def insert_daily_data(index_name, date, close):
    """
    Insert daily closing price data
    
    Parameters:
    -----------
    index_name : str
        Index name
    date : str or datetime
        Trade date (YYYY-MM-DD)
    close : float
        Closing price
    
    Returns:
    --------
    bool : Success status
    """
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Convert date to string if needed
        if isinstance(date, datetime):
            date = date.strftime('%Y-%m-%d')
        
        cursor.execute('''
            INSERT OR REPLACE INTO daily_data 
            (index_name, date, close)
            VALUES (?, ?, ?)
        ''', (index_name, date, float(close)))
        
        conn.commit()
        return True
    
    except Exception as e:
        logger.error(f"Error inserting daily data: {str(e)}")
        return False
    
    finally:
        if conn:
            conn.close()

def bulk_insert_daily_data(index_name, data_df):
    """
    Insert multiple closing price records
    
    Parameters:
    -----------
    index_name : str
        Index name
    data_df : pd.DataFrame
        DataFrame with columns: date, close
    
    Returns:
    --------
    bool : Success status
    """
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Ensure index exists
        insert_index(index_name)
        
        for _, row in data_df.iterrows():
            date = row['Date'] if 'Date' in row.index else row.get('date')
            
            if isinstance(date, pd.Timestamp):
                date = date.strftime('%Y-%m-%d')
            elif isinstance(date, datetime):
                date = date.strftime('%Y-%m-%d')
            
            cursor.execute('''
                INSERT OR REPLACE INTO daily_data 
                (index_name, date, close)
                VALUES (?, ?, ?)
            ''', (
                index_name,
                date,
                float(row.get('Close', 0))
            ))
        
        conn.commit()
        logger.info(f"Inserted {len(data_df)} records for {index_name}")
        return True
    
    except Exception as e:
        logger.error(f"Error bulk inserting data: {str(e)}")
        return False
    
    finally:
        if conn:
            conn.close()

def get_index_data(index_name, days=180):
    """
    Retrieve index closing price data from database
    
    Parameters:
    -----------
    index_name : str
        Index name
    days : int
        Number of days of historical data (default 180 = 6 months)
    
    Returns:
    --------
    pd.DataFrame : Historical data with Date and Close columns
    """
    try:
        conn = get_db_connection()
        if not conn:
            return pd.DataFrame()
        
        query = '''
            SELECT date, close
            FROM daily_data
            WHERE index_name = ?
            AND date >= datetime('now', '-' || ? || ' days')
            ORDER BY date ASC
        '''
        
        df = pd.read_sql_query(query, conn, params=(index_name, days))
        
        if not df.empty:
            df['Date'] = pd.to_datetime(df['date'])
            df = df[['Date', 'close']]
            df.columns = ['Date', 'Close']
            logger.info(f"Retrieved {len(df)} records for {index_name}")
        
        return df
    
    except Exception as e:
        logger.error(f"Error retrieving index data: {str(e)}")
        return pd.DataFrame()
    
    finally:
        if conn:
            conn.close()

def get_all_indices():
    """
    Get list of all indices in database
    
    Returns:
    --------
    list : List of index names
    """
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cursor = conn.cursor()
        cursor.execute('SELECT index_name FROM indices ORDER BY index_name')
        
        indices = [row[0] for row in cursor.fetchall()]
        return indices
    
    except Exception as e:
        logger.error(f"Error retrieving indices: {str(e)}")
        return []
    
    finally:
        if conn:
            conn.close()

def get_data_status():
    """
    Get status of data in database
    
    Returns:
    --------
    dict : Data status information
    """
    try:
        conn = get_db_connection()
        if not conn:
            return {}
        
        cursor = conn.cursor()
        
        status = {}
        
        # Get count of indices
        cursor.execute('SELECT COUNT(*) FROM indices')
        status['total_indices'] = cursor.fetchone()[0]
        
        # Get total records
        cursor.execute('SELECT COUNT(*) FROM daily_data')
        status['total_records'] = cursor.fetchone()[0]
        
        # Get date range
        cursor.execute('SELECT MIN(date), MAX(date) FROM daily_data')
        result = cursor.fetchone()
        status['earliest_date'] = result[0]
        status['latest_date'] = result[1]
        
        # Get records per index
        cursor.execute('''
            SELECT index_name, COUNT(*) as count, MIN(date), MAX(date)
            FROM daily_data
            GROUP BY index_name
            ORDER BY index_name
        ''')
        
        status['indices_detail'] = []
        for row in cursor.fetchall():
            status['indices_detail'].append({
                'index_name': row[0],
                'records': row[1],
                'from_date': row[2],
                'to_date': row[3]
            })
        
        return status
    
    except Exception as e:
        logger.error(f"Error getting data status: {str(e)}")
        return {}
    
    finally:
        if conn:
            conn.close()

def clear_old_data(days_to_keep=180):
    """
    Clear data older than specified days
    
    Parameters:
    -----------
    days_to_keep : int
        Number of days to keep (default 180 = 6 months)
    
    Returns:
    --------
    int : Number of records deleted
    """
    try:
        conn = get_db_connection()
        if not conn:
            return 0
        
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM daily_data
            WHERE date < datetime('now', '-' || ? || ' days')
        ''', (days_to_keep,))
        
        conn.commit()
        deleted = cursor.rowcount
        logger.info(f"Deleted {deleted} old records (keeping last {days_to_keep} days)")
        return deleted
    
    except Exception as e:
        logger.error(f"Error clearing old data: {str(e)}")
        return 0
    
    finally:
        if conn:
            conn.close()

def get_indices_with_data():
    """
    Get list of indices that have data in database
    
    Returns:
    --------
    list : List of index names with data
    """
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cursor = conn.cursor()
        cursor.execute('''
            SELECT DISTINCT index_name FROM daily_data ORDER BY index_name
        ''')
        
        indices = [row[0] for row in cursor.fetchall()]
        return indices
    
    except Exception as e:
        logger.error(f"Error retrieving indices with data: {str(e)}")
        return []
    
    finally:
        if conn:
            conn.close()


def get_data_date_range():
    """
    Get the date range of data in database
    
    Returns:
    --------
    dict : Start date and end date
    """
    try:
        conn = get_db_connection()
        if not conn:
            return {'start_date': None, 'end_date': None}
        
        cursor = conn.cursor()
        cursor.execute('''
            SELECT MIN(date) as start_date, MAX(date) as end_date
            FROM daily_data
        ''')
        
        result = cursor.fetchone()
        
        return {
            'start_date': result[0],
            'end_date': result[1]
        }
    
    except Exception as e:
        logger.error(f"Error getting date range: {str(e)}")
        return {'start_date': None, 'end_date': None}
    
    finally:
        if conn:
            conn.close()
