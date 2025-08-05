-- Clear Database Script
-- This script will remove ALL data from the Lenovo diagnostics database
-- Use this to test the updated battery validation and serial number extraction

-- WARNING: This will delete ALL data in the database!
-- Make sure you have backups if needed before running this script

-- Delete all data from child tables first (due to foreign key constraints)
DELETE FROM test_results;
DELETE FROM motherboard;
DELETE FROM storage;
DELETE FROM memory;
DELETE FROM cpu;
DELETE FROM display;
DELETE FROM battery;

-- Delete all data from the main system_info table last
DELETE FROM system_info;

-- Reset the auto-increment sequences (if using SERIAL columns)
-- This ensures new uploads start with ID 1 again
ALTER SEQUENCE IF EXISTS system_info_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS battery_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS display_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS cpu_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS memory_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS storage_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS motherboard_id_seq RESTART WITH 1;
ALTER SEQUENCE IF EXISTS test_results_id_seq RESTART WITH 1;

-- Verify all tables are empty
SELECT 'system_info' as table_name, COUNT(*) as row_count FROM system_info
UNION ALL
SELECT 'battery' as table_name, COUNT(*) as row_count FROM battery
UNION ALL
SELECT 'display' as table_name, COUNT(*) as row_count FROM display
UNION ALL
SELECT 'cpu' as table_name, COUNT(*) as row_count FROM cpu
UNION ALL
SELECT 'memory' as table_name, COUNT(*) as row_count FROM memory
UNION ALL
SELECT 'storage' as table_name, COUNT(*) as row_count FROM storage
UNION ALL
SELECT 'motherboard' as table_name, COUNT(*) as row_count FROM motherboard
UNION ALL
SELECT 'test_results' as table_name, COUNT(*) as row_count FROM test_results;

-- Show message
SELECT 'Database cleared successfully! All tables are now empty.' as status;
