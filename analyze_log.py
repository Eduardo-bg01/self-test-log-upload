import os
from log_database_uploader import LenovoLogDatabaseUploader

def analyze_log_file(log_path):
    """Analyze a log file to see what tests it contains"""
    print(f"Analyzing log file: {log_path}")
    print("=" * 80)
    
    if not os.path.exists(log_path):
        print(f"❌ Log file not found: {log_path}")
        return
    
    # Create uploader instance
    uploader = LenovoLogDatabaseUploader()
    
    try:
        # Load the log file
        uploader.load_log_file(log_path)
        print("✅ Log file loaded successfully")
        
        # Parse all data
        all_data = uploader.parse_all_data()
        
        # Show system info
        system_info = all_data['system_info']
        print(f"\n📱 SYSTEM INFO:")
        print(f"  Serial: {system_info['machine_serial_number']}")
        print(f"  Model: {system_info['machine_model']}")
        print(f"  BIOS: {system_info['bios_version']}")
        print(f"  Start: {system_info['start_time']}")
        print(f"  Finish: {system_info['finish_time']}")
        
        # Show what components were found
        print(f"\n🔍 COMPONENTS DETECTED:")
        components = ['battery', 'display', 'cpu', 'memory', 'storage', 'motherboard']
        for component in components:
            data = all_data.get(component)
            if data and data != {}:
                print(f"  ✅ {component.upper()}: Found")
                if component == 'battery':
                    print(f"     Health: {data.get('health_percentage', 'Unknown')}%")
                    print(f"     Status: {data.get('validation_status', 'Unknown')}")
            else:
                print(f"  ❌ {component.upper()}: Not found")
        
        # Show test results
        test_data = all_data['test_results']
        if test_data and test_data.get('tests'):
            print(f"\n🧪 TESTS FOUND ({test_data['total_tests']} total):")
            for test in test_data['tests']:
                status = "✅ PASSED" if test['passed'] else "❌ FAILED"
                print(f"  {test['test_name']}: {status}")
        else:
            print(f"\n❌ NO TESTS FOUND")
        
        # Show raw log content sections
        print(f"\n📄 LOG CONTENT ANALYSIS:")
        lines = uploader.log_content.split('\n')
        
        # Look for diagnostic sections
        sections = []
        for line in lines:
            if '+++' in line and 'DIAGNOSTIC' in line:
                sections.append(line.strip())
        
        if sections:
            print("  Diagnostic sections found:")
            for section in sections:
                print(f"    - {section}")
        else:
            print("  ❌ No diagnostic sections found")
        
        # Look for test markers
        test_markers = []
        for line in lines:
            if 'START TESTS' in line or 'STOP TESTS' in line:
                test_markers.append(line.strip())
        
        if test_markers:
            print("  Test markers found:")
            for marker in test_markers[:10]:  # Show first 10
                print(f"    - {marker}")
        else:
            print("  ❌ No test markers found")
            
        # Look for individual test results
        test_results_lines = []
        for line in lines:
            if 'STOP ' in line and ('PASSED' in line or 'FAILED' in line):
                test_results_lines.append(line.strip())
        
        print(f"\n  Individual test results found: {len(test_results_lines)}")
        for result in test_results_lines[:10]:  # Show first 10
            print(f"    - {result}")
            
    except Exception as e:
        print(f"Error analyzing log: {e}")

if __name__ == "__main__":
    log_path = r"D:\Log\PF3G44S9-2025-08-07-202945.log"
    analyze_log_file(log_path)
