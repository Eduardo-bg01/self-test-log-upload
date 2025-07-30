#!/usr/bin/env python3
"""
Standalone script to process real JSON diagnostic data from D:\Log directory
No dependencies - completely self-contained
"""

import json
import re
import os
from datetime import datetime

def validate_battery(health_percentage, cycles):
    """Validate battery based on Lenovo standards"""
    if health_percentage is None or cycles is None:
        return {'status': 'UNKNOWN', 'message': 'Insufficient data for validation'}
    
    if health_percentage >= 80 and cycles <= 500:
        return {'status': 'GOOD', 'message': 'Battery health is within acceptable range'}
    elif health_percentage >= 70 and cycles <= 800:
        return {'status': 'FAIR', 'message': 'Battery health is fair, consider replacement soon'}
    else:
        return {'status': 'POOR', 'message': 'Battery health is poor, replacement recommended'}

def parse_battery_from_json(modules):
    """Parse battery information from JSON modules"""
    for module in modules:
        if module.get('name') == 'BATTERY':
            diagnostics = module.get('diagnostics', [])
            if diagnostics:
                diag = diagnostics[0]
                details = diag.get('properties', {})
                
                # Get battery metrics
                design_capacity = details.get('DESIGN_CAPACITY')
                full_charge_capacity = details.get('FULL_CHARGE_CAPACITY')
                cycles = details.get('CYCLE_COUNT')
                
                # Extract numeric part from values like '57000mWh (3691mAh)'
                def extract_num(val):
                    if not val:
                        return 0
                    m = re.match(r'(\d+)', str(val))
                    return int(m.group(1)) if m else 0
                
                design_capacity_num = extract_num(design_capacity)
                full_charge_capacity_num = extract_num(full_charge_capacity)
                cycles_num = extract_num(cycles)
                
                # Calculate health percentage
                health_percentage = None
                if design_capacity_num > 0 and full_charge_capacity_num > 0:
                    health_percentage = (full_charge_capacity_num / design_capacity_num) * 100
                
                # Validate battery
                validation = validate_battery(health_percentage, cycles_num)
                
                return {
                    'serial_number': details.get('SERIAL_NUMBER'),
                    'manufacturer': details.get('MANUFACTURER'),
                    'design_capacity': f"{design_capacity_num} mWh",
                    'full_charge_capacity': f"{full_charge_capacity_num} mWh",
                    'cycles': cycles_num,
                    'health_percentage': f"{health_percentage:.2f}%" if health_percentage else None,
                    'validation_status': validation['status'],
                    'validation_message': validation['message']
                }
    return None

def parse_display_from_json(modules):
    """Parse display information from JSON modules"""
    for module in modules:
        if module.get('name') == 'DISPLAY':
            diagnostics = module.get('diagnostics', [])
            if diagnostics:
                diag = diagnostics[0]
                details = diag.get('properties', {})
                
                # Parse resolution
                resolution = details.get('NATIVE_RESOLUTION', '0x0')
                width, height = 0, 0
                if 'x' in str(resolution):
                    try:
                        width, height = map(int, str(resolution).split('x'))
                    except ValueError:
                        pass
                
                return {
                    'name': diag.get('udi'),
                    'manufacturer_id': details.get('MANUFACTURER_ID'),
                    'width': width,
                    'height': height,
                    'edid_version': details.get('EDID_VERSION')
                }
    return None

def parse_cpu_from_json(modules):
    """Parse CPU information from JSON modules"""
    for module in modules:
        if module.get('name') == 'CPU':
            diagnostics = module.get('diagnostics', [])
            if diagnostics:
                diag = diagnostics[0]
                details = diag.get('properties', {})
                return {
                    'model': details.get('CPU_MODEL'),
                    'manufacturer': details.get('CPU_VENDOR'),
                    'cores': details.get('CPU_CORES'),
                    'threads': details.get('CPU_THREADS'),
                    'current_speed': details.get('CPU_CURRENT_SPEED'),
                    'cache_l1': details.get('CPU_CACHE_L1'),
                    'cache_l2': details.get('CPU_CACHE_L2'),
                    'cache_l3': details.get('CPU_CACHE_L3')
                }
    return None

def parse_memory_from_json(modules):
    """Parse memory information from JSON modules"""
    for module in modules:
        if module.get('name') == 'MEMORY':
            diagnostics = module.get('diagnostics', [])
            if diagnostics:
                diag = diagnostics[0]
                details = diag.get('properties', {})
                resources = diag.get('resources', [])
                
                # Count memory modules and get sample info from first module
                memory_modules = [res for res in resources if res.get('name') == 'bank']
                module_count = len(memory_modules)
                
                # Get details from first module as representative sample
                sample_module = memory_modules[0] if memory_modules else {}
                
                # Calculate total memory from individual modules
                total_memory_gb = 0
                module_size_str = sample_module.get('SIZE', '0 GB')
                if module_size_str:
                    # Extract numeric part from '4.000 GB'
                    size_match = re.match(r'([\d.]+)', module_size_str)
                    if size_match:
                        module_size_gb = float(size_match.group(1))
                        total_memory_gb = module_size_gb * module_count
                
                memory_info = {
                    'total_memory_gb': f"{total_memory_gb:.0f} GB" if total_memory_gb > 0 else "Unknown",
                    'module_count': module_count,
                    'module_type': sample_module.get('TYPE'),
                    'module_manufacturer': sample_module.get('MANUFACTURER'),
                    'module_size_gb': module_size_str,
                    'module_speed': sample_module.get('SPEED'),
                    'module_part_number': sample_module.get('PART_NUMBER')
                }
                
                return memory_info
    return None

def parse_storage_from_json(modules):
    """Parse storage information from JSON modules"""
    for module in modules:
        if module.get('name') == 'STORAGE':
            diagnostics = module.get('diagnostics', [])
            if diagnostics:
                diag = diagnostics[0]
                details = diag.get('properties', {})
                return {
                    'model': details.get('MODEL'),
                    'serial_number': details.get('SERIAL'),
                    'size': details.get('SIZE'),
                    'protocol': details.get('PROTOCOL'),
                    'firmware': details.get('FIRMWARE'),
                    'temperature': details.get('TEMPERATURE')
                }
    return None

def parse_motherboard_from_json(modules):
    """Parse motherboard information from JSON modules"""
    for module in modules:
        if module.get('name') == 'MOTHERBOARD':
            diagnostics = module.get('diagnostics', [])
            if diagnostics:
                diag = diagnostics[0]
                details = diag.get('properties', {})
                return {
                    'usb_controllers': details.get('MOTHERBOARD_USB_HOST_CONTROLLER_COUNT'),
                    'pci_devices': details.get('MOTHERBOARD_PCI_DEVICE_COUNT'),
                    'rtc_present': details.get('MOTHERBOARD_REAL_TIME_CLOCK_PRESENT')
                }
    return None

def parse_test_results_from_json(modules):
    """Parse test results from JSON modules"""
    test_results = []
    total_tests = 0
    passed_tests = 0
    
    for module in modules:
        module_name = module.get('name', 'UNKNOWN')
        diagnostics = module.get('diagnostics', [])
        
        for diag in diagnostics:
            tests = diag.get('tests', [])
            for test in tests:
                test_name = f"{module_name} - {test.get('name', 'UNKNOWN')}"
                result = test.get('result', 'UNKNOWN')
                passed = result.upper() in ['SUCCESS', 'PASSED', 'PASS']
                
                test_results.append({
                    'test_name': test_name,
                    'result': result,
                    'passed': passed
                })
                
                total_tests += 1
                if passed:
                    passed_tests += 1
    
    return {
        'tests': test_results,
        'total_tests': total_tests,
        'passed_tests': passed_tests,
        'failed_tests': total_tests - passed_tests
    }

def show_database_upload_preview(json_data, filename):
    """Show what data would be uploaded to the database"""
    print(f"{'='*80}")
    print(f"DATABASE UPLOAD PREVIEW - REAL DATA FROM D:\\Log")
    print(f"File: {filename}")
    print(f"{'='*80}")

    modules = json_data['iterations'][0]['modules']

    # System Info
    print('🔹 SYSTEM_INFO Table:')
    print(f'  serial_number: {json_data.get("machine_serial_number", "NULL")}')
    print(f'  machine_model: {json_data.get("machine_model", "NULL")}')
    print(f'  machine_type_model: {json_data.get("machine_type_model", "NULL")}')
    print(f'  bios_version: {json_data.get("bios_version", "NULL")}')
    print(f'  app_version: {json_data.get("application_version", "NULL")}')
    print(f'  execution_type: {json_data.get("execution_type", "NULL")}')
    print(f'  start_time: {json_data.get("start_time", "NULL")}')
    print(f'  finish_time: {json_data.get("finish_time", "NULL")}')

    # Parse and display battery
    battery = parse_battery_from_json(modules)
    if battery:
        print('🔋 BATTERY Table:')
        print(f'  serial_number: {battery["serial_number"]}')
        print(f'  manufacturer: {battery["manufacturer"]}')
        print(f'  design_capacity: {battery["design_capacity"]}')
        print(f'  full_charge_capacity: {battery["full_charge_capacity"]}')
        print(f'  cycles: {battery["cycles"]}')
        print(f'  health_percentage: {battery["health_percentage"]}')
        print(f'  validation_status: {battery["validation_status"]}')
        print(f'  validation_message: {battery["validation_message"]}')

    # Parse and display other components
    display = parse_display_from_json(modules)
    if display:
        print('🖥️ DISPLAY Table:')
        print(f'  name: {display["name"]}')
        print(f'  manufacturer_id: {display["manufacturer_id"]}')
        print(f'  width: {display["width"]}')
        print(f'  height: {display["height"]}')
        print(f'  edid_version: {display["edid_version"]}')

    cpu = parse_cpu_from_json(modules)
    if cpu:
        print('🖥️ CPU Table:')
        print(f'  model: {cpu["model"]}')
        print(f'  manufacturer: {cpu["manufacturer"]}')
        print(f'  cores: {cpu["cores"]}')
        print(f'  threads: {cpu["threads"]}')
        print(f'  current_speed: {cpu["current_speed"]}')
        print(f'  cache_l1: {cpu["cache_l1"]}')
        print(f'  cache_l2: {cpu["cache_l2"]}')
        print(f'  cache_l3: {cpu["cache_l3"]}')

    memory = parse_memory_from_json(modules)
    if memory:
        print('💾 MEMORY Table:')
        print(f'  total_memory: {memory["total_memory_gb"]}')
        print(f'  module_count: {memory["module_count"]}')
        print(f'  module_type: {memory["module_type"]}')
        print(f'  module_manufacturer: {memory["module_manufacturer"]}')
        print(f'  module_size: {memory["module_size_gb"]} (per module)')
        print(f'  module_speed: {memory["module_speed"]}')
        print(f'  module_part_number: {memory["module_part_number"]}')

    storage = parse_storage_from_json(modules)
    if storage:
        print('💽 STORAGE Table:')
        print(f'  model: {storage["model"]}')
        print(f'  serial_number: {storage["serial_number"]}')
        print(f'  size: {storage["size"]}')
        print(f'  protocol: {storage["protocol"]}')
        print(f'  firmware: {storage["firmware"]}')
        print(f'  temperature: {storage["temperature"]}')

    motherboard = parse_motherboard_from_json(modules)
    if motherboard:
        print('🏗️ MOTHERBOARD Table:')
        print(f'  usb_controllers: {motherboard["usb_controllers"]}')
        print(f'  pci_devices: {motherboard["pci_devices"]}')
        print(f'  rtc_present: {motherboard["rtc_present"]}')

    # Parse and display test results
    test_data = parse_test_results_from_json(modules)
    if test_data:
        print(f'🧪 TEST_RESULTS Table ({test_data["total_tests"]} tests):')
        print(f'  Total Tests: {test_data["total_tests"]}')
        print(f'  Passed: {test_data["passed_tests"]}')
        print(f'  Failed: {test_data["failed_tests"]}')
        print('  Sample tests:')
        
        # Show first 5 tests as examples
        for i, test in enumerate(test_data["tests"][:5]):
            status = "✓ PASS" if test["passed"] else "✗ FAIL"
            print(f'    {status} {test["test_name"]}: {test["result"]}')
        
        if len(test_data["tests"]) > 5:
            print(f'    ... and {len(test_data["tests"]) - 5} more tests')

    # Upload Status
    print('✅ UPLOAD STATUS: Ready for database insertion')
    print(f'📂 Source File: {filename}')
    print(f'🕒 Processed At: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

def main():
    """Process real JSON data from D:\Log directory"""
    log_path = r"D:\Log"
    
    if not os.path.exists(log_path):
        print(f"Directory {log_path} not found!")
        return
    
    # Find JSON files
    json_files = []
    for root, dirs, files in os.walk(log_path):
        for file in files:
            if file.lower().endswith('.json'):
                json_files.append(os.path.join(root, file))
    
    if not json_files:
        print(f"No JSON files found in {log_path}")
        return
    
    print(f"Found {len(json_files)} JSON file(s) in {log_path}:")
    for i, file_path in enumerate(json_files, 1):
        print(f"  {i}. {os.path.basename(file_path)}")
    
    # Process the first JSON file
    json_file_path = json_files[0]
    filename = os.path.basename(json_file_path)
    
    try:
        print(f"\nProcessing: {filename}")
        with open(json_file_path, 'r') as f:
            json_data = json.load(f)
        
        # Show database upload preview
        show_database_upload_preview(json_data, filename)
        
    except Exception as e:
        print(f"Error processing {filename}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
