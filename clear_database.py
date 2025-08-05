import psycopg2
import logging

from log_database_uploader import load_db_config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clear_database():
    try:
        # Load database configuration
        db_config = load_db_config()
        
        # Connect to database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Delete all data from child tables first (due to foreign key constraints)
        tables_to_clear = [
            'test_results',
            'motherboard', 
            'storage',
            'memory',
            'cpu',
            'display',
            'battery',
            'system_info'  # Main table last
        ]
        
        for table in tables_to_clear:
            cursor.execute(f"DELETE FROM {table}")
            deleted_count = cursor.rowcount
            logger.info(f"Deleted {deleted_count} rows from {table}")
        
        # Reset auto-increment sequences
        sequences = [
            'system_info_id_seq',
            'battery_id_seq',
            'display_id_seq', 
            'cpu_id_seq',
            'memory_id_seq',
            'storage_id_seq',
            'motherboard_id_seq',
            'test_results_id_seq'
        ]
        
        for seq in sequences:
            try:
                cursor.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1")
            except psycopg2.Error as e:
                logger.warning(f"Could not reset sequence {seq}: {e}")
        
        # Commit all changes
        conn.commit()
        
        logger.info("✅ Database cleared successfully!")
        
        # Close connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error clearing database: {e}")
        raise

if __name__ == "__main__":
    print("🗑️  CLEARING DATABASE - This will delete ALL data!")
    response = input("Are you sure you want to continue? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        clear_database()
        print("\n🎉 Database clearing complete! You can now test the updated code.")
    else:
        print("Operation cancelled.")
