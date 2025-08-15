import re
import os
import psycopg2
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_db_config(config_file_path='dataBaseInfo.config'):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, config_file_path)
    
        with open(config_path, 'r') as config_file:
            config_content = config_file.read()

        config_namespace = {}
        exec(config_content, config_namespace)
        
        return config_namespace['db_config_lenovo']  # Use lenovo config
    
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise
    except KeyError:
        logger.error(f"db_config_lenovo not found in config file: {config_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        raise

class LenovoLogDatabaseUploader:
    
    def __init__(self, db_config=None):
        self.log_content = None
        self.filename = None
        self.db_config = db_config or load_db_config()
        self.conn = None
    
    def connect_db(self):
        """Connect to the database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def close_db(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def validate_battery(self, health_percentage, cycles):
        """Validate battery based on strict Lenovo standards - both health and cycles must pass"""
        if health_percentage is None:
            return {'status': 'UNKNOWN', 'message': 'Insufficient data for validation'}
        
        # Check health percentage (must be >= 80%)
        health_passed = health_percentage >= 80
        
        # Check cycle count (must be < 750 cycles)
        cycles_passed = cycles is not None and cycles < 750
        
        # Determine status based on both conditions
        if health_passed and cycles_passed:
            return {'status': 'GOOD', 'message': f'Battery health is good ({health_percentage:.1f}%, {cycles} cycles)'}
        elif not health_passed and not cycles_passed:
            return {'status': 'POOR', 'message': f'Battery failed: health below 80% ({health_percentage:.1f}%) and cycles too high ({cycles})'}
        elif not health_passed:
            return {'status': 'POOR', 'message': f'Battery failed: health below 80% ({health_percentage:.1f}%)'}
        else:  # not cycles_passed
            return {'status': 'FAIR', 'message': f'Battery degraded: cycle count too high ({cycles} cycles)'}
    
    
    def extract_field_value(self, line):
        """Extract value from a line in format 'FIELD_NAME: value'"""
        if ':' in line:
            return line.split(':', 1)[1].strip()
        return ''
    
    def load_log_file(self, file_path):
        """Load and parse a log file with proper encoding handling"""
        self.filename = os.path.basename(file_path)
        self._file_path = file_path  # Store full path for JSON companion file lookup
        
        # List of encodings to try in order
        encodings_to_try = [
            'utf-16',
            'utf-16-le', 
            'utf-16-be',
            'utf-8',
            'utf-8-sig',  # UTF-8 with BOM
            'latin-1',
            'cp1252'
        ]
        
        for encoding in encodings_to_try:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    self.log_content = f.read()
                return True
            except (UnicodeDecodeError, UnicodeError) as e:
                logger.debug(f"Failed to read {self.filename} with {encoding}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error reading {self.filename} with {encoding}: {e}")
                continue
        
        # If all encodings fail, try reading as binary and decode with error handling
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read()
            
            # Try to decode with UTF-8 and replace/ignore errors
            try:
                self.log_content = raw_data.decode('utf-8', errors='replace')
                logger.warning(f"Loaded {self.filename} with UTF-8 and error replacement")
                return True
            except Exception:
                # Last resort: try latin-1 which can decode any byte sequence
                self.log_content = raw_data.decode('latin-1', errors='replace')
                logger.warning(f"Loaded {self.filename} with latin-1 fallback")
                return True
                
        except Exception as e:
            logger.error(f"Error loading {self.filename}: {e}")
            return False
    
    def parse_battery(self):
        """Parse battery information from log content"""
        if not self.log_content:
            return None
            
        lines = self.log_content.split('\n')
        in_battery_section = False
        battery_lines = []
        
        for line in lines:
            line = line.strip()
            if '+++ ' in line and 'BATTERY' in line:
                in_battery_section = True
                battery_lines = []
                continue
            elif '--- BATTERY' in line:
                in_battery_section = False
                break
            elif in_battery_section and line:
                battery_lines.append(line)
        
        if not battery_lines:
            return None
        
        # Parse battery data
        battery_data = {}
        for line in battery_lines:
            if line.startswith('MANUFACTURER:'):
                battery_data['manufacturer'] = self.extract_field_value(line)
            elif line.startswith('SERIAL_NUMBER:'):
                battery_data['serial_number'] = self.extract_field_value(line)
            elif line.startswith('CYCLE_COUNT:'):
                cycle_str = self.extract_field_value(line)
                battery_data['cycles'] = int(cycle_str) if cycle_str.isdigit() else 0
            elif line.startswith('DESIGN_CAPACITY:'):
                capacity_str = self.extract_field_value(line)
                # Extract numeric part from '57000mWh (3691mAh)'
                match = re.match(r'(\d+)', capacity_str)
                battery_data['design_capacity_num'] = int(match.group(1)) if match else 0
                battery_data['design_capacity'] = capacity_str
            elif line.startswith('FULL_CHARGE_CAPACITY:'):
                capacity_str = self.extract_field_value(line)
                # Extract numeric part from '50270mWh (3059mAh)'
                match = re.match(r'(\d+)', capacity_str)
                battery_data['full_charge_capacity_num'] = int(match.group(1)) if match else 0
                battery_data['full_charge_capacity'] = capacity_str
        
        # Calculate health percentage
        health_percentage = None
        if (battery_data.get('design_capacity_num', 0) > 0 and 
            battery_data.get('full_charge_capacity_num', 0) > 0):
            health_percentage = (battery_data['full_charge_capacity_num'] / 
                               battery_data['design_capacity_num']) * 100
        
        # Validate battery
        validation = self.validate_battery(health_percentage, battery_data.get('cycles'))
        
        return {
            'serial_number': battery_data.get('serial_number', 'Unknown'),
            'manufacturer': battery_data.get('manufacturer', 'Unknown'),
            'design_capacity': f"{battery_data.get('design_capacity_num', 0)} mWh",
            'full_charge_capacity': f"{battery_data.get('full_charge_capacity_num', 0)} mWh",
            'cycles': battery_data.get('cycles', 0),
            'health_percentage': health_percentage,  # Keep as numeric for database
            'validation_status': validation['status'],
            'validation_message': validation['message']
        }
    
    def parse_display(self):
        """Parse display information from log content"""
        if not self.log_content:
            return None
            
        lines = self.log_content.split('\n')
        in_display_section = False
        display_lines = []
        
        for line in lines:
            line = line.strip()
            if '+++ ' in line and 'DISPLAY' in line:
                in_display_section = True
                display_lines = []
                continue
            elif '--- DISPLAY' in line:
                in_display_section = False
                break
            elif in_display_section and line:
                display_lines.append(line)
        
        if not display_lines:
            return None
        
        # Parse display data
        display_data = {}
        for line in display_lines:
            if line.startswith('UDI:'):
                display_data['name'] = self.extract_field_value(line)
            elif line.startswith('MANUFACTURER_ID:'):
                display_data['manufacturer_id'] = self.extract_field_value(line)
            elif line.startswith('EDID_VERSION:'):
                display_data['edid_version'] = self.extract_field_value(line)
            elif line.startswith('MAX_RESOLUTION:'):
                resolution_str = self.extract_field_value(line)
                # Parse '1920 x 1200 pixels'
                match = re.match(r'(\d+)\s*x\s*(\d+)', resolution_str)
                if match:
                    display_data['width'] = int(match.group(1))
                    display_data['height'] = int(match.group(2))
                else:
                    display_data['width'] = 0
                    display_data['height'] = 0
        
        return {
            'name': display_data.get('name', 'Unknown'),
            'manufacturer_id': display_data.get('manufacturer_id', 'Unknown'),
            'width': display_data.get('width', 0),
            'height': display_data.get('height', 0),
            'edid_version': display_data.get('edid_version', 'Unknown')
        }
    
    def parse_cpu(self):
        """Parse CPU information from log content"""
        if not self.log_content:
            return None
            
        lines = self.log_content.split('\n')
        in_cpu_section = False
        cpu_lines = []
        
        for line in lines:
            line = line.strip()
            if '+++ ' in line and 'CPU' in line:
                in_cpu_section = True
                cpu_lines = []
                continue
            elif '--- CPU' in line:
                in_cpu_section = False
                break
            elif in_cpu_section and line:
                cpu_lines.append(line)
        
        if not cpu_lines:
            return None
        
        # Parse CPU data
        cpu_data = {}
        for line in cpu_lines:
            if line.startswith('CPU_MODEL:'):
                cpu_data['model'] = self.extract_field_value(line)
            elif line.startswith('CPU_VENDOR:'):
                cpu_data['manufacturer'] = self.extract_field_value(line)
            elif line.startswith('CPU_CORES:'):
                cores_str = self.extract_field_value(line)
                cpu_data['cores'] = int(cores_str) if cores_str.isdigit() else 0
            elif line.startswith('CPU_THREADS:'):
                threads_str = self.extract_field_value(line)
                cpu_data['threads'] = int(threads_str) if threads_str.isdigit() else 0
            elif line.startswith('CPU_CURRENT_SPEED:'):
                cpu_data['current_speed'] = self.extract_field_value(line)
            elif line.startswith('CPU_CACHE_L1:'):
                cpu_data['cache_l1'] = self.extract_field_value(line)
            elif line.startswith('CPU_CACHE_L2:'):
                cpu_data['cache_l2'] = self.extract_field_value(line)
            elif line.startswith('CPU_CACHE_L3:'):
                cpu_data['cache_l3'] = self.extract_field_value(line)
        
        return {
            'model': cpu_data.get('model', 'Unknown'),
            'manufacturer': cpu_data.get('manufacturer', 'Unknown'),
            'cores': cpu_data.get('cores', 0),
            'threads': cpu_data.get('threads', 0),
            'current_speed': cpu_data.get('current_speed', 'Unknown'),
            'cache_l1': cpu_data.get('cache_l1', 'Unknown'),
            'cache_l2': cpu_data.get('cache_l2', 'Unknown'),
            'cache_l3': cpu_data.get('cache_l3', 'Unknown')
        }
    
    def parse_memory(self):
        """Parse memory information from log content"""
        if not self.log_content:
            return None
            
        lines = self.log_content.split('\n')
        in_memory_section = False
        memory_lines = []
        
        for line in lines:
            line = line.strip()
            if '+++ ' in line and 'MEMORY' in line:
                in_memory_section = True
                memory_lines = []
                continue
            elif '--- MEMORY' in line:
                in_memory_section = False
                break
            elif in_memory_section and line:
                memory_lines.append(line)
        
        if not memory_lines:
            return None
        
        # Parse memory data from log format
        memory_data = {}
        module_count = 0
        
        for line in memory_lines:
            if line.startswith('TOTAL_PHYSICAL_MEMORY:'):
                total_mb = self.extract_field_value(line).replace(' MB', '')
                if total_mb.isdigit():
                    total_gb = int(total_mb) / 1024
                    memory_data['total_memory'] = f"{total_gb:.0f} GB"
            elif line.startswith('TYPE:') and 'module_type' not in memory_data:
                memory_data['module_type'] = self.extract_field_value(line)
            elif line.startswith('MANUFACTURER:') and 'module_manufacturer' not in memory_data:
                memory_data['module_manufacturer'] = self.extract_field_value(line)
            elif line.startswith('MEMORY_CURRENT_SPEED:') and 'module_speed' not in memory_data:
                memory_data['module_speed'] = self.extract_field_value(line)
            elif line.startswith('PART_NUMBER:') and 'module_part_number' not in memory_data:
                memory_data['module_part_number'] = self.extract_field_value(line)
            elif line.startswith('SIZE:'):
                # Count each SIZE entry as a memory module
                module_count += 1
                if 'module_size' not in memory_data:
                    size_value = self.extract_field_value(line)
                    memory_data['module_size'] = size_value
        
        return {
            'total_memory_gb': memory_data.get('total_memory', 'Unknown'),
            'module_count': module_count,
            'module_type': memory_data.get('module_type', 'Unknown'),
            'module_manufacturer': memory_data.get('module_manufacturer', 'Unknown'),
            'module_size_gb': memory_data.get('module_size', 'Unknown'),
            'module_speed': memory_data.get('module_speed', 'Unknown'),
            'module_part_number': memory_data.get('module_part_number', 'Unknown')
        }
    
    def parse_storage(self):
        """Parse storage information from log content"""
        if not self.log_content:
            return None
            
        lines = self.log_content.split('\n')
        in_storage_section = False
        storage_lines = []
        
        for line in lines:
            line = line.strip()
            if '+++ ' in line and 'STORAGE' in line:
                in_storage_section = True
                storage_lines = []
                continue
            elif '--- STORAGE' in line:
                in_storage_section = False
                break
            elif in_storage_section and line:
                storage_lines.append(line)
        
        if not storage_lines:
            return None
        
        # Parse storage data from log format
        storage_data = {}
        for line in storage_lines:
            if line.startswith('MODEL_NUMBER:'):
                storage_data['model'] = self.extract_field_value(line)
            elif line.startswith('SERIAL_NUMBER:'):
                storage_data['serial_number'] = self.extract_field_value(line)
            elif line.startswith('MANUFACTURER:'):
                storage_data['manufacturer'] = self.extract_field_value(line)
            elif line.startswith('DEVICE_TYPE:'):
                storage_data['protocol'] = self.extract_field_value(line)
            elif line.startswith('FIRMWARE_REVISION:'):
                storage_data['firmware'] = self.extract_field_value(line)
            elif line.startswith('TEMPERATURE:'):
                temp_str = self.extract_field_value(line)
                # Extract just the numeric part and unit (e.g., "40 C")
                storage_data['temperature'] = temp_str
            elif line.startswith('NUMBER_LOGICAL_BLOCKS:'):
                # Calculate size from logical blocks
                blocks_str = self.extract_field_value(line).replace(' blocks', '')
                if blocks_str.isdigit():
                    blocks = int(blocks_str)
                    # Assuming 512 bytes per block, convert to GB
                    size_gb = (blocks * 512) / (1024 * 1024 * 1024)
                    storage_data['size'] = f"{size_gb:.0f} GB"
        
        return {
            'model': storage_data.get('model', 'Unknown'),
            'serial_number': storage_data.get('serial_number', 'Unknown'),
            'size': storage_data.get('size', 'Unknown'),
            'protocol': storage_data.get('protocol', 'Unknown'),
            'firmware': storage_data.get('firmware', 'Unknown'),
            'temperature': storage_data.get('temperature', 'Unknown')
        }
    
    def parse_motherboard(self):
        """Parse motherboard information from log content"""
        if not self.log_content:
            return None
            
        lines = self.log_content.split('\n')
        in_motherboard_section = False
        motherboard_lines = []
        
        for line in lines:
            line = line.strip()
            if '+++ ' in line and 'MOTHERBOARD' in line:
                in_motherboard_section = True
                motherboard_lines = []
                continue
            elif '--- MOTHERBOARD' in line:
                in_motherboard_section = False
                break
            elif in_motherboard_section and line:
                motherboard_lines.append(line)
        
        if not motherboard_lines:
            return None
        
        # Count PCI devices and USB controllers from the motherboard section
        pci_device_count = 0
        usb_controller_count = 0
        rtc_present = 'Unknown'
        
        # First, search entire log for USB controller count
        for line in lines:
            if line.startswith('NUMBER_USB_HOST_CONTROLLERS:'):
                usb_controllers_from_count = self.extract_field_value(line)
                if usb_controllers_from_count.isdigit():
                    usb_controller_count = int(usb_controllers_from_count)
                    break
        
        # Search motherboard section for additional info
        for line in motherboard_lines:
            if 'PCI_INDEX:' in line:
                pci_device_count += 1
            elif line.startswith('RTC_PRESENCE:'):
                rtc_present = self.extract_field_value(line)
        
        # If we didn't find USB count from direct field, count from CLASS lines
        if usb_controller_count == 0:
            for line in motherboard_lines:
                if 'CLASS:' in line and 'USB' in line.upper():
                    usb_controller_count += 1
        
        return {
            'usb_controllers': usb_controller_count if usb_controller_count > 0 else 'Unknown',
            'pci_devices': pci_device_count if pci_device_count > 0 else 'Unknown',
            'rtc_present': rtc_present
        }
    
    def parse_test_results(self):
        """Parse test results from log content"""
        if not self.log_content:
            return None
            
        lines = self.log_content.split('\n')
        test_results = []
        total_tests = 0
        passed_tests = 0
        
        current_section = None
        
        for line in lines:
            line = line.strip()
            
            # Detect section starts
            if '+++ ' in line:
                # Extract section name from "+++ 20250729T105545UTC BATTERY QUICK DIAGNOSTIC 1753786545"
                parts = line.split()
                if len(parts) >= 4:
                    current_section = parts[2]  # BATTERY, DISPLAY, etc.
            
            # Parse test results
            if 'STOP ' in line and ('PASSED' in line or 'FAILED' in line or 'NOT APPLICABLE' in line or 'SUCCESS' in line):
                # Example: "20250729T105545UTC STOP HEALTH_TEST PASSED 0 S" or "20250807T202117UTC STOP INTERNAL_SPEAKER_TEST SUCCESS 15 S"
                parts = line.split()
                if len(parts) >= 4:
                    test_name_part = None
                    result_part = None
                    
                    # Find test name and result
                    for i, part in enumerate(parts):
                        if part == 'STOP' and i + 1 < len(parts):
                            test_name_part = parts[i + 1]
                        elif part in ['PASSED', 'FAILED', 'NOT APPLICABLE', 'SUCCESS']:
                            result_part = part
                            break
                    
                    if test_name_part and result_part:
                        test_name = f"{current_section} - {test_name_part}" if current_section else test_name_part
                        result = result_part
                        passed = result in ['PASSED', 'SUCCESS']
                        
                        test_results.append({
                            'test_name': test_name,
                            'result': result,
                            'passed': passed
                        })
                        
                        total_tests += 1
                        if passed:
                            passed_tests += 1
        
        # Add battery validation tests based on battery health analysis
        battery_data = self.parse_battery()
        if battery_data and battery_data.get('health_percentage') is not None:
            # Create battery health test based on validation
            validation_status = battery_data.get('validation_status', 'UNKNOWN')
            
            # Overall battery health test
            battery_health_passed = validation_status == 'GOOD'
            test_results.append({
                'test_name': 'BATTERY - HEALTH_TEST',
                'result': 'PASSED' if battery_health_passed else 'FAILED',
                'passed': battery_health_passed
            })
            total_tests += 1
            if battery_health_passed:
                passed_tests += 1
            
            # Specific health percentage test
            health_percentage = battery_data.get('health_percentage', 0)
            health_percentage_passed = health_percentage >= 80
            test_results.append({
                'test_name': 'BATTERY - HEALTH_PERCENTAGE_TEST',
                'result': 'PASSED' if health_percentage_passed else 'FAILED',
                'passed': health_percentage_passed
            })
            total_tests += 1
            if health_percentage_passed:
                passed_tests += 1
            
            # Battery cycle count test
            cycles = battery_data.get('cycles', 0)
            if cycles is not None:
                cycles_passed = cycles < 750
                test_results.append({
                    'test_name': 'BATTERY - CYCLE_COUNT_TEST',
                    'result': 'PASSED' if cycles_passed else 'FAILED',
                    'passed': cycles_passed
                })
                total_tests += 1
                if cycles_passed:
                    passed_tests += 1
        
        return {
            'tests': test_results,
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': total_tests - passed_tests
        }
    
    def parse_system_info(self):
        """Parse system information from the beginning of the log"""
        if not self.log_content:
            return None
            
        lines = self.log_content.split('\n')
        system_data = {}
        
        # Extract machine serial number from filename first (e.g., PF4CC0SB-2025-07-30-115447.log)
        machine_serial_from_filename = None
        if self.filename:
            # Extract exactly the first 8 characters before the first hyphen as the machine serial
            filename_parts = self.filename.split('-')
            if len(filename_parts) > 0:
                potential_serial = filename_parts[0]
                # Take exactly 8 characters for machine serial (standard Lenovo format)
                if len(potential_serial) >= 8 and potential_serial.isalnum():
                    machine_serial_from_filename = potential_serial[:8]  # Take first 8 characters
                elif len(potential_serial) >= 6 and potential_serial.isalnum():
                    # Fallback for shorter serials, but prefer 8-character format
                    machine_serial_from_filename = potential_serial
        
        # Also check for SYSTEM_SERIAL_NUMBER or MACHINE_SERIAL_NUMBER in the log content (as backup)
        machine_serial_from_log = None
        for line in lines[:30]:  # Check first 30 lines
            line = line.strip()
            if (line.startswith('SYSTEM_SERIAL_NUMBER:') or 
                line.startswith('MACHINE_SERIAL_NUMBER:') or
                line.startswith('COMPUTER_SERIAL_NUMBER:')):
                potential_serial = self.extract_field_value(line)
                # Validate it's a proper machine serial (6+ chars, alphanumeric)
                if len(potential_serial) >= 6 and potential_serial.isalnum():
                    machine_serial_from_log = potential_serial
                    break
        
        # PRIORITIZE FILENAME - use filename as primary source, log content as backup only
        if machine_serial_from_filename:
            system_data['machine_serial_number'] = machine_serial_from_filename
            logger.debug(f"Using machine serial from filename: {machine_serial_from_filename}")
        elif machine_serial_from_log:
            system_data['machine_serial_number'] = machine_serial_from_log
            logger.debug(f"Using machine serial from log content (filename failed): {machine_serial_from_log}")
        else:
            # Last resort: use filename without validation
            if self.filename and '-' in self.filename:
                fallback_serial = self.filename.split('-')[0]
                system_data['machine_serial_number'] = fallback_serial
                logger.warning(f"Using fallback machine serial from filename: {fallback_serial}")
            else:
                system_data['machine_serial_number'] = 'Unknown'
                logger.warning(f"Could not determine machine serial for file: {self.filename}")
        
        # Parse header information (first 30 lines) for other system details
        for line in lines[:30]:  # Extend search to first 30 lines
            line = line.strip()
            # Skip component serial numbers
            if line.startswith('BIOS_VERSION:'):
                system_data['bios_version'] = self.extract_field_value(line)
            elif line.startswith('MACHINE_MODEL:'):
                system_data['machine_model'] = self.extract_field_value(line)
            elif line.startswith('APPLICATION_VERSION:'):
                system_data['application_version'] = self.extract_field_value(line)
            elif line.startswith('EXECUTION_TYPE:'):
                system_data['execution_type'] = self.extract_field_value(line)
        
        # Search entire log for 8S_CODE which contains machine type
        machine_type_model = 'N/A'
        
        # First, try to get machine type from companion JSON file if it exists
        json_file_path = self.filename.replace('.log', '.json')
        json_dir = os.path.dirname(self._file_path) if hasattr(self, '_file_path') else r'D:\Log'
        full_json_path = os.path.join(json_dir, json_file_path)
        
        if os.path.exists(full_json_path):
            try:
                import json
                with open(full_json_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    machine_type_model = json_data.get('machine_type_model', 'N/A')
            except Exception:
                pass  # Keep N/A if JSON extraction fails
            #If no JSON file found, keep machine_type_model as 'N/A'
        
        # Extract timestamps from the log with proper parsing
        start_time = None
        finish_time = None
        start_timestamp = None
        finish_timestamp = None
        elapsed_seconds = None
        test_date = None
        
        # Find start time (first timestamp in log)
        for line in lines:
            if '+++ ' in line and 'UTC' in line:
                match = re.search(r'(\d{8}T\d{6})UTC', line)
                if match and start_time is None:
                    raw_start_time = match.group(1)
                    try:
                        from datetime import datetime
                        start_timestamp = datetime.strptime(raw_start_time, '%Y%m%dT%H%M%S')
                        # Format as "day/month/year hour:minutes" (e.g., "07/08/2025 20:34")
                        start_time = start_timestamp.strftime('%d/%m/%Y %H:%M')
                        test_date = start_timestamp.date()
                    except:
                        start_time = raw_start_time  # Fallback to original format
                    break
        
        # Find finish time (last timestamp in log) 
        for reverse_line in reversed(lines):
            match = re.search(r'(\d{8}T\d{6})UTC', reverse_line)
            if match:
                raw_finish_time = match.group(1)
                # Parse to proper datetime for database
                try:
                    from datetime import datetime
                    finish_timestamp = datetime.strptime(raw_finish_time, '%Y%m%dT%H%M%S')
                    # Format as "day/month/year hour:minutes" (e.g., "07/08/2025 20:34")
                    finish_time = finish_timestamp.strftime('%d/%m/%Y %H:%M')
                except:
                    finish_time = raw_finish_time  # Fallback to original format
                break
        
        #Calculate elapsed time in seconds
        if start_timestamp and finish_timestamp:
            elapsed_seconds = int((finish_timestamp - start_timestamp).total_seconds())
        
        return {
            'machine_serial_number': system_data.get('machine_serial_number', 'Unknown'),
            'machine_model': system_data.get('machine_model', 'Unknown'),
            'machine_type_model': machine_type_model,
            'bios_version': system_data.get('bios_version', 'Unknown'),
            'application_version': system_data.get('application_version', 'Unknown'),
            'execution_type': system_data.get('execution_type', 'Unknown'),
            'start_time': start_time or 'Unknown',
            'finish_time': finish_time or 'Unknown',
            'test_start_timestamp': start_timestamp,
            'test_finish_timestamp': finish_timestamp,
            'test_elapsed_seconds': elapsed_seconds,
            'test_date': test_date
        }
    
    def parse_all_data(self):
        if not self.log_content:
            return None
        
        return {
            'system_info': self.parse_system_info(),
            'battery': self.parse_battery(),
            'display': self.parse_display(),
            'cpu': self.parse_cpu(),
            'memory': self.parse_memory(),
            'storage': self.parse_storage(),
            'motherboard': self.parse_motherboard(),
            'test_results': self.parse_test_results()
        }
    
    def upload_to_database(self, data=None):
        if not self.conn:
            raise Exception("Database connection not established. Call connect_db() first.")
        
        if data is None:
            data = self.parse_all_data()
        
        if not data or not data.get('system_info'):
            logger.error("No data to upload or missing system info")
            return None
        
        cursor = self.conn.cursor()
        
        try:
            # 1. Insert system_info (main table) - Allow multiple test records per machine
            system_info = data['system_info']
            
            # Direct INSERT without DELETE - allows multiple test sessions per machine
            cursor.execute("""
                INSERT INTO system_info (serial_number, machine_model, machine_type_model, bios_version, app_version, execution_type, start_time, finish_time, created_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                RETURNING id
            """, (
                system_info['machine_serial_number'],
                system_info['machine_model'],
                system_info['machine_type_model'],
                system_info['bios_version'],
                system_info['application_version'],
                system_info['execution_type'],
                system_info.get('start_time'),
                system_info.get('finish_time'),
                system_info.get('test_start_timestamp') or datetime.now()
            ))
            
            system_id = cursor.fetchone()[0]
            
            # Log timing information
            if system_info.get('test_start_timestamp') and system_info.get('test_finish_timestamp'):
                elapsed_time = system_info.get('test_elapsed_seconds', 0)
                elapsed_mins = elapsed_time // 60
                elapsed_secs = elapsed_time % 60
                logger.info(f"\nTest Duration: {elapsed_mins}m {elapsed_secs}s")
            
            # 2. Insert battery if available
            if data['battery']:
                battery = data['battery']
                
                cursor.execute("""
                    INSERT INTO battery (system_id, serial_number, manufacturer, design_capacity, full_charge_capacity, cycles, health_percentage, validation_status, validation_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    system_id,
                    battery['serial_number'],
                    battery['manufacturer'],
                    battery['design_capacity'],
                    battery['full_charge_capacity'],
                    battery['cycles'],
                    f"{battery['health_percentage']:.2f}%" if battery['health_percentage'] else None,
                    battery['validation_status'],
                    battery['validation_message']
                ))
                
                logger.info(f"Battery validation - Serial: {battery['serial_number']}, "
                           f"Status: {battery['validation_status']}, Health: {battery.get('health_percentage', 'N/A')}%, "
                           f"Cycles: {battery.get('cycles', 'N/A')}, Message: {battery['validation_message']}")
            
            # 3. Insert display if available
            if data['display']:
                display = data['display']
                
                cursor.execute("""
                    INSERT INTO display (system_id, name, manufacturer_id, width, height, edid_version)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    system_id,
                    display.get('name'),
                    display.get('manufacturer_id'),
                    display.get('width'),
                    display.get('height'),
                    display.get('edid_version')
                ))
            
            # 4. Insert CPU if available
            if data['cpu']:
                cpu = data['cpu']
                
                cursor.execute("""
                    INSERT INTO cpu (system_id, model, manufacturer, cores, threads, current_speed, cache_l1, cache_l2, cache_l3)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    system_id,
                    cpu.get('model'),
                    cpu.get('manufacturer'),
                    cpu.get('cores'),
                    cpu.get('threads'),
                    cpu.get('current_speed'),
                    cpu.get('cache_l1'),
                    cpu.get('cache_l2'),
                    cpu.get('cache_l3')
                ))
            
            # 5. Insert memory if available
            if data['memory']:
                memory = data['memory']
                
                cursor.execute("""
                    INSERT INTO memory (system_id, total_memory, module_count, module_type, module_manufacturer, module_size, module_speed, module_part_number)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    system_id,
                    memory['total_memory_gb'],
                    memory['module_count'],
                    memory['module_type'],
                    memory['module_manufacturer'],
                    memory['module_size_gb'],
                    memory['module_speed'],
                    memory['module_part_number']
                ))
            
            # 6. Insert storage if available
            if data['storage']:
                storage = data['storage']
                
                cursor.execute("""
                    INSERT INTO storage (system_id, model, serial_number, size, protocol, firmware, temperature)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    system_id,
                    storage.get('model'),
                    storage.get('serial_number'),
                    storage.get('size'),
                    storage.get('protocol'),
                    storage.get('firmware'),
                    storage.get('temperature')
                ))
            
            # 7. Insert motherboard if available
            if data['motherboard']:
                mb = data['motherboard']
                
                cursor.execute("""
                    INSERT INTO motherboard (system_id, usb_controllers, pci_devices, rtc_present)
                    VALUES (%s, %s, %s, %s)
                """, (
                    system_id,
                    mb.get('usb_controllers'),
                    mb.get('pci_devices'),
                    mb.get('rtc_present')
                ))
            
            # 8. Insert test results if available
            if data['test_results'] and data['test_results']['tests']:
                
                # Get timing data from system_info for all test results
                test_start_time = system_info.get('test_start_timestamp')
                test_finish_time = system_info.get('test_finish_timestamp')
                test_elapsed_seconds = system_info.get('test_elapsed_seconds')
                test_date = system_info.get('test_date')
                
                for test in data['test_results']['tests']:
                    cursor.execute("""
                        INSERT INTO test_results (system_id, test_name, result, passed, test_start_time, test_finish_time, test_elapsed_seconds, test_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        system_id,
                        test['test_name'],
                        test['result'],
                        test['passed'],
                        test_start_time,
                        test_finish_time,
                        test_elapsed_seconds,
                        test_date
                    ))
            
            self.conn.commit()
            return system_id
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Database upload failed: {e}")
            raise
        finally:
            cursor.close()
    
    def process_and_upload_log_file(self, file_path):
        try:
            if not self.load_log_file(file_path):
                logger.error(f"Failed to load log file: {file_path}")
                return None
            
            data = self.parse_all_data()
            if not data:
                logger.error(f"Failed to parse data from: {file_path}")
                return None
                
            if not data.get('system_info') or not data['system_info'].get('machine_serial_number'):
                logger.error(f"No valid system info found in: {file_path}")
                return None
            
            # Upload to database
            system_id = self.upload_to_database(data)
            
            if system_id:
                logger.info(f"Successfully processed and uploaded {self.filename} with system_id: {system_id}")
                return system_id
            else:
                logger.error(f"Failed to upload data for {self.filename}")
                return None
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return None
    
    def process_log_directory(self, log_directory, pattern="*.log"):
        import glob
        
        if not os.path.exists(log_directory):
            logger.error(f"Directory {log_directory} not found!")
            return []
        
        # Find all log files
        search_pattern = os.path.join(log_directory, "**", pattern)
        log_files = glob.glob(search_pattern, recursive=True)
        
        if not log_files:
            logger.warning(f"No log files found in {log_directory}")
            return []
        
        # Filter out obviously invalid files
        valid_log_files = []
        for log_file in log_files:
            filename = os.path.basename(log_file)
            # Skip files that don't look like proper diagnostic logs
            if (filename.lower() == 'log.log' or 
                len(filename) < 10 or 
                not filename.endswith('.log')):
                logger.warning(f"Skipping invalid file: {filename}")
                continue
            valid_log_files.append(log_file)
        
        logger.info(f"Found {len(valid_log_files)} valid log file(s) to process (skipped {len(log_files) - len(valid_log_files)} invalid files)")
        
        results = []
        for log_file in valid_log_files:
            try:
                system_id = self.process_and_upload_log_file(log_file)
                if system_id:
                    results.append({
                        'file': log_file,
                        'system_id': system_id,
                        'status': 'success'
                    })
                else:
                    results.append({
                        'file': log_file,
                        'system_id': None,
                        'status': 'failed'
                    })
            except Exception as e:
                logger.error(f"Failed to process {log_file}: {e}")
                results.append({
                    'file': log_file,
                    'system_id': None,
                    'status': 'error',
                    'error': str(e)
                })
        
        # Summary
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] in ['failed', 'error']]
        
        logger.info(f"Processing complete: {len(successful)} successful, {len(failed)} failed")
        
        return results


def main():
    """Example usage of the LenovoLogDatabaseUploader"""
    log_path = r"D:\Log"
    
    try:
        # Create uploader instance
        uploader = LenovoLogDatabaseUploader()
        
        # Connect to database
        uploader.connect_db()
        
        # Process all log files in directory
        results = uploader.process_log_directory(log_path)

        
        for result in results:
            status_icon = "✅" if result['status'] == 'success' else "❌"
            file_name = os.path.basename(result['file'])
            system_id = result.get('system_id', 'N/A')
            print(f"{status_icon} {file_name}: system_id={system_id}")
        
        # Close database connection
        uploader.close_db()
        
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
