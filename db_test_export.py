"""
Database Test Program - Export data to CSV for manual verification
Dumps all database contents to CSV format for verification
"""
import sqlite3
import pandas as pd
from datetime import datetime
import os
from pathlib import Path

# Database file path
DB_PATH = os.path.join(os.path.dirname(__file__), 'indices_data.db')
EXPORT_DIR = os.path.join(os.path.dirname(__file__), 'db_exports')

def ensure_export_dir():
    """Create export directory if it doesn't exist"""
    Path(EXPORT_DIR).mkdir(exist_ok=True)
    return EXPORT_DIR

def get_db_connection():
    """Get database connection"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {str(e)}")
        return None

def export_daily_data_to_csv():
    """Export all daily data to CSV"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        # Read all daily data
        query = 'SELECT index_name, date, close FROM daily_data ORDER BY index_name, date'
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("⚠️  No data found in daily_data table")
            conn.close()
            return False
        
        # Export to CSV
        export_dir = ensure_export_dir()
        csv_file = os.path.join(export_dir, 'daily_data_export.csv')
        df.to_csv(csv_file, index=False)
        
        print(f"✅ Exported daily_data to: {csv_file}")
        print(f"   - Total records: {len(df)}")
        print(f"   - Columns: {', '.join(df.columns.tolist())}")
        print(f"   - Date range: {df['date'].min()} to {df['date'].max()}")
        
        conn.close()
        return True
    
    except Exception as e:
        print(f"❌ Error exporting daily data: {str(e)}")
        return False

def export_indices_summary_to_csv():
    """Export summary of indices and record counts to CSV"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        # Get summary statistics
        query = '''
        SELECT 
            index_name,
            COUNT(*) as total_records,
            MIN(date) as start_date,
            MAX(date) as end_date,
            MIN(close) as min_close,
            MAX(close) as max_close,
            ROUND(AVG(close), 2) as avg_close
        FROM daily_data
        GROUP BY index_name
        ORDER BY index_name
        '''
        
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("⚠️  No indices found in database")
            conn.close()
            return False
        
        # Export to CSV
        export_dir = ensure_export_dir()
        csv_file = os.path.join(export_dir, 'indices_summary.csv')
        df.to_csv(csv_file, index=False)
        
        print(f"\n✅ Exported indices_summary to: {csv_file}")
        print(f"   - Total indices: {len(df)}")
        print("\n   Index Summary:")
        for idx, row in df.iterrows():
            print(f"     • {row['index_name']}: {row['total_records']} records ({row['start_date']} to {row['end_date']})")
            print(f"       Close range: {row['min_close']} - {row['max_close']}, Avg: {row['avg_close']}")
        
        conn.close()
        return True
    
    except Exception as e:
        print(f"❌ Error exporting summary: {str(e)}")
        return False

def export_indices_table_to_csv():
    """Export indices configuration table to CSV"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        query = 'SELECT index_name FROM indices ORDER BY index_name'
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("⚠️  No indices found in indices table")
            conn.close()
            return False
        
        export_dir = ensure_export_dir()
        csv_file = os.path.join(export_dir, 'indices_config.csv')
        df.to_csv(csv_file, index=False)
        
        print(f"\n✅ Exported indices_config to: {csv_file}")
        print(f"   - Configured indices: {len(df)}")
        
        conn.close()
        return True
    
    except Exception as e:
        print(f"❌ Error exporting indices config: {str(e)}")
        return False

def verify_data_integrity():
    """Verify database integrity"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        print("\n" + "="*60)
        print("📊 DATABASE INTEGRITY VERIFICATION")
        print("="*60)
        
        # Check if database file exists
        if os.path.exists(DB_PATH):
            db_size = os.path.getsize(DB_PATH) / 1024  # Size in KB
            print(f"\n✅ Database file exists: {DB_PATH}")
            print(f"   File size: {db_size:.2f} KB")
        else:
            print(f"\n❌ Database file not found: {DB_PATH}")
            return False
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"\n✅ Tables found: {len(tables)}")
        for table in tables:
            print(f"   • {table[0]}")
        
        # Check indices table
        cursor.execute("SELECT COUNT(*) FROM indices")
        indices_count = cursor.fetchone()[0]
        print(f"\n✅ Indices configured: {indices_count}")
        
        cursor.execute("SELECT index_name FROM indices ORDER BY index_name")
        for row in cursor.fetchall():
            print(f"   • {row[0]}")
        
        # Check daily_data table
        cursor.execute("SELECT COUNT(*) FROM daily_data")
        records_count = cursor.fetchone()[0]
        print(f"\n✅ Total daily_data records: {records_count}")
        
        # Check for each index
        cursor.execute('''
            SELECT index_name, COUNT(*) as count
            FROM daily_data
            GROUP BY index_name
            ORDER BY index_name
        ''')
        
        print("\n   Records per index:")
        for row in cursor.fetchall():
            print(f"   • {row[0]}: {row[1]} records")
        
        # Check date range
        cursor.execute("SELECT MIN(date), MAX(date) FROM daily_data")
        result = cursor.fetchone()
        if result[0] and result[1]:
            print(f"\n✅ Data date range: {result[0]} to {result[1]}")
        else:
            print(f"\n⚠️  No data available in database")
        
        # Check for NULL values
        cursor.execute('''
            SELECT COUNT(*) FROM daily_data 
            WHERE index_name IS NULL OR date IS NULL OR close IS NULL
        ''')
        null_count = cursor.fetchone()[0]
        if null_count == 0:
            print(f"\n✅ Data quality: No NULL values found")
        else:
            print(f"\n⚠️  Found {null_count} records with NULL values")
        
        conn.close()
        print("\n" + "="*60)
        return True
    
    except Exception as e:
        print(f"\n❌ Error verifying database: {str(e)}")
        return False

def main():
    """Main function"""
    print("\n" + "="*60)
    print("🗄️  DATABASE TEST & EXPORT UTILITY")
    print("="*60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Verify database integrity
    if not verify_data_integrity():
        print("\n❌ Database verification failed")
        return
    
    # Export data
    print("\n" + "="*60)
    print("📤 EXPORTING DATA TO CSV")
    print("="*60)
    
    success = True
    success = export_daily_data_to_csv() and success
    success = export_indices_summary_to_csv() and success
    success = export_indices_table_to_csv() and success
    
    # Summary
    if success:
        export_dir = ensure_export_dir()
        print("\n" + "="*60)
        print("✅ EXPORT COMPLETE")
        print("="*60)
        print(f"\nExport directory: {export_dir}")
        print("\nGenerated files:")
        for file in sorted(os.listdir(export_dir)):
            file_path = os.path.join(export_dir, file)
            file_size = os.path.getsize(file_path) / 1024  # Size in KB
            print(f"  • {file} ({file_size:.2f} KB)")
        print("\nYou can now open these CSV files in Excel or other tools for manual verification.")
    else:
        print("\n⚠️  Some exports failed")
    
    print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    main()
