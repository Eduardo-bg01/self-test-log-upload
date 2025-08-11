#!/usr/bin/env python3
"""
Quick script to check database schema and then machine tests
"""
import psycopg2
from log_database_uploader import load_db_config

def check_database_schema():
    """Check the actual column names in database tables"""
    try:
        # Load database config
        db_config = load_db_config()
        
        # Connect to database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("Database Schema Check:")
        print("=" * 50)
        
        # Check system_info table columns
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'system_info' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print("system_info table columns:")
        for col in columns:
            print(f"  - {col[0]}")
        
        # Check test_results table columns
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'test_results' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print("\ntest_results table columns:")
        for col in columns:
            print(f"  - {col[0]}")
            
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

def check_machine_tests(serial_number):
    """Check all test data for a specific machine using correct column names"""
    try:
        # Load database config
        db_config = load_db_config()
        
        # Connect to database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print(f"\nChecking test data for machine: {serial_number}")
        print("=" * 60)
        
        # Check if machine exists in system_info (using correct column name) - get most recent record by ID
        cursor.execute("SELECT * FROM system_info WHERE serial_number = %s ORDER BY id DESC LIMIT 1", (serial_number,))
        system_info = cursor.fetchone()
        
        if not system_info:
            print(f"❌ Machine {serial_number} not found in database!")
            
            # Check what machines are in the database
            cursor.execute("SELECT serial_number FROM system_info ORDER BY serial_number LIMIT 10")
            existing_machines = cursor.fetchall()
            print("\nExisting machines in database:")
            for machine in existing_machines:
                print(f"  - {machine[0]}")
            return
        
        print("✅ Machine found in database")
        print(f"Machine Model: {system_info[2] if len(system_info) > 2 else 'Unknown'}")
        print(f"BIOS Version: {system_info[4] if len(system_info) > 4 else 'Unknown'}")
        print()
        
        # Check test results (using system_id from system_info)
        system_id = system_info[0]  # First column is id
        cursor.execute("""
            SELECT test_name, result, passed 
            FROM test_results 
            WHERE system_id = %s 
            ORDER BY test_name
        """, (system_id,))
        
        test_results = cursor.fetchall()
        
        if not test_results:
            print("❌ No test results found for this machine!")
            print("\nPossible reasons:")
            print("1. Log file didn't contain test results")
            print("2. Test parsing failed")
            print("3. Tests were not properly formatted in log")
            return
        
        print(f"📊 Test Results ({len(test_results)} tests found):")
        print("-" * 50)
        
        passed_count = 0
        failed_count = 0
        
        for test_name, result, passed in test_results:
            status = "✅ PASSED" if passed else "❌ FAILED"
            print(f"{test_name}: {status} ({result})")
            if passed:
                passed_count += 1
            else:
                failed_count += 1
        
        print(f"\nSummary: {passed_count} passed, {failed_count} failed, {len(test_results)} total")
        
        # Check if battery data exists
        cursor.execute("SELECT * FROM battery WHERE serial_number = %s", (serial_number,))
        battery_data = cursor.fetchone()
        
        if battery_data:
            print(f"\n🔋 Battery found - checking validation status...")
            print(f"Battery Health: {battery_data[6] if len(battery_data) > 6 else 'Unknown'}%")
            print(f"Validation Status: {battery_data[7] if len(battery_data) > 7 else 'Unknown'}")
        else:
            print("\n❌ No battery data found")
        
        # Check what other components were detected
        cursor.execute("SELECT * FROM display WHERE serial_number = %s", (serial_number,))
        display_data = cursor.fetchone()
        print(f"🖥️  Display: {'Found' if display_data else 'Not found'}")
        
        cursor.execute("SELECT * FROM cpu WHERE serial_number = %s", (serial_number,))
        cpu_data = cursor.fetchone()
        print(f"🖥️  CPU: {'Found' if cpu_data else 'Not found'}")
        
        cursor.execute("SELECT * FROM memory WHERE serial_number = %s", (serial_number,))
        memory_data = cursor.fetchone()
        print(f"💾 Memory: {'Found' if memory_data else 'Not found'}")
        
        cursor.execute("SELECT * FROM storage WHERE serial_number = %s", (serial_number,))
        storage_data = cursor.fetchone()
        print(f"💽 Storage: {'Found' if storage_data else 'Not found'}")
        
        cursor.execute("SELECT * FROM motherboard WHERE serial_number = %s", (serial_number,))
        motherboard_data = cursor.fetchone()
        print(f"🏗️  Motherboard: {'Found' if motherboard_data else 'Not found'}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking machine tests: {e}")

if __name__ == "__main__":
    check_database_schema()
    import sys
    serial = sys.argv[1] if len(sys.argv) > 1 else "PF3G44S9"
    check_machine_tests(serial)
