import re
import psycopg2
from datetime import datetime
import json
import logging
import os
import glob

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
        
        return config_namespace['db_config_lenovo']  # Changed to use lenovo config
    
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise
    except KeyError:
        logger.error(f"db_config_lenovo not found in config file: {config_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        raise

class DiagnosticLogParser:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None

    #Connect to the database
    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    #Close the database connection
    def close_db(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    #parses the JSON file to be able to read and extract the information
    #and returns a dictionary with the parsed data
    def parse_json_file(self, json_file_path):
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
            
            data = {}
            
            #System information
            data['serial_number'] = json_data.get('machine_serial_number')
            data['bios_version'] = json_data.get('bios_version')
            data['machine_model'] = json_data.get('machine_model')
            data['machine_type_model'] = json_data.get('machine_type_model')  # For motherboard specs
            data['app_version'] = json_data.get('application_version')
            data['execution_type'] = json_data.get('execution_type')
            
            #Time for time started (convert from the JSON format)
            data['execution_start'] = self._parse_json_timestamp(json_data.get('start_time'))
            
            #Computer components - extract from iterations[0].modules
            if json_data.get('iterations') and len(json_data['iterations']) > 0:
                modules = json_data['iterations'][0].get('modules', [])
                data['battery'] = self._parse_json_battery(modules)
                data['display'] = self._parse_json_display(modules)
                data['cpu'] = self._parse_json_cpu(modules)
                data['memory'] = self._parse_json_memory(modules)
                data['storage'] = self._parse_json_storage(modules)
                data['motherboard'] = self._parse_json_motherboard(modules)
                
                #Test results from all modules
                data['test_results'] = self._parse_json_test_results(modules)
                data['test_summary'] = self._parse_json_test_summary(json_data['iterations'][0])
            else:
                # Initialize empty if no iterations
                data['battery'] = None
                data['display'] = None
                data['cpu'] = None
                data['memory'] = []
                data['storage'] = None
                data['motherboard'] = None
                data['test_results'] = []
                data['test_summary'] = None
            
            return data
            
        except FileNotFoundError:
            logger.error(f"JSON file not found: {json_file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON file {json_file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing JSON file {json_file_path}: {e}")
            raise
    
    def _extract_field(self, content, pattern):
        match = re.search(pattern, content)
        return match.group(1).strip() if match else None
    
    #Parse JSON timestamp format (e.g., "20250729T105545")
    def _parse_json_timestamp(self, timestamp_str):
        if not timestamp_str:
            return None
        try:
            return datetime.strptime(timestamp_str, '%Y%m%dT%H%M%S')
        except ValueError:
            logger.warning(f"Could not parse timestamp: {timestamp_str}")
            return None
    
    #Execution time
    def _extract_execution_time(self, content):
        match = re.search(r'(\d{8}T\d{6}UTC)', content)
        if match:
            time_str = match.group(1)
            return datetime.strptime(time_str, '%Y%m%dT%H%M%SUTC')
        return None
    
    #battery information
    def _parse_battery(self, content):
        battery_section = re.search(r'\+\+\+ .+? BATTERY QUICK DIAGNOSTIC .+?\n(.+?)\n--- BATTERY QUICK DIAGNOSTIC', content, re.DOTALL)
        if not battery_section:
            return None
        
        battery_text = battery_section.group(1)
        
        #Battery fields extraction
        design_capacity = self._extract_numeric(battery_text, r'DESIGN_CAPACITY:\s*(\d+)')
        full_charge_capacity = self._extract_numeric(battery_text, r'FULL_CHARGE_CAPACITY:\s*(\d+)')
        cycles_str = self._extract_field(battery_text, r'CYCLE_COUNT:\s*(.+)')
        
        #Convert cycles to numeric
        cycles = None
        if cycles_str:
            try:
                cycles = int(cycles_str.strip())
            except ValueError:
                logger.warning(f"Could not parse cycle count: {cycles_str}")
                cycles = 0

        #Battery health percentage calculation
        battery_health_percentage = None
        if design_capacity and full_charge_capacity and design_capacity > 0:
            battery_health_percentage = (full_charge_capacity / design_capacity) * 100
        
        #Validation of battery health and cycles based on lenovo standards
        battery_status = self._validate_battery(battery_health_percentage, cycles)
        
        battery_data = {
            'serial_number': self._extract_field(battery_text, r'SERIAL_NUMBER:\s*(.+)'),
            'manufacturer': self._extract_field(battery_text, r'MANUFACTURER:\s*(.+)'),
            'design_capacity': design_capacity,
            'full_charge_capacity': full_charge_capacity,
            'cycles': cycles_str,
            'cycles_numeric': cycles,
            'device_name': self._extract_field(battery_text, r'UDI:\s*(.+)'),
            'health_percentage': battery_health_percentage,
            'validation_status': battery_status['status'],
            'validation_message': battery_status['message']
        }
        
        return battery_data
    
    #display info
    def _parse_display(self, content):
        display_section = re.search(r'\+\+\+ .+? DISPLAY DIAGNOSTIC .+?\n(.+?)\n--- DISPLAY DIAGNOSTIC', content, re.DOTALL)
        if not display_section:
            return None
        
        display_text = display_section.group(1)
        
        width_match = re.search(r'MAX_RESOLUTION:\s*(\d+)\s*x\s*(\d+)', display_text)
        width = int(width_match.group(1)) if width_match else 0
        height = int(width_match.group(2)) if width_match else 0
        
        display_data = {
            'name': self._extract_field(display_text, r'MODEL_NAME:\s*(.+)'),
            'serial_number': self._extract_field(display_text, r'UDI:\s*(.+)'),
            'manufacturer_id': self._extract_field(display_text, r'MANUFACTURER_ID:\s*(.+)'),
            'manufacturer_code': self._extract_field(display_text, r'MANUFACTURER_ID:\s*(.+)'),
            'width': width,
            'height': height,
            'max_nits': 300.0,
            'min_nits': 0.5
        }
        
        return display_data
    
    #cpu info
    def _parse_cpu(self, content):
        cpu_section = re.search(r'\+\+\+ .+? CPU QUICK DIAGNOSTIC .+?\n(.+?)\n--- CPU QUICK DIAGNOSTIC', content, re.DOTALL)
        if not cpu_section:
            return None
        
        cpu_text = cpu_section.group(1)
        
        cpu_data = {
            'model': self._extract_field(cpu_text, r'CPU_MODEL:\s*(.+)'),
            'vendor': self._extract_field(cpu_text, r'CPU_VENDOR:\s*(.+)'),
            'cores': self._extract_numeric(cpu_text, r'CPU_CORES:\s*(\d+)'),
            'threads': self._extract_numeric(cpu_text, r'CPU_THREADS:\s*(\d+)'),
            'max_speed': self._extract_field(cpu_text, r'CPU_MAX_SPEED:\s*(.+)'),
            'features': self._extract_field(cpu_text, r'CPU_FEATURES:\s*(.+)')
        }
        
        return cpu_data
    
    #memory information
    def _parse_memory(self, content):
        memory_section = re.search(r'\+\+\+ .+? MEMORY QUICK DIAGNOSTIC .+?\n(.+?)\n--- MEMORY QUICK DIAGNOSTIC', content, re.DOTALL)
        if not memory_section:
            return []
        
        memory_text = memory_section.group(1)
        
        #Total physical memory
        total_memory = self._extract_numeric(memory_text, r'TOTAL_PHYSICAL_MEMORY:\s*(\d+)')
        
        #individual memory modules
        memory_modules = []
        smbios_sections = re.findall(r'ORIGIN: SMBIOS\n(.*?)(?=ORIGIN: SMBIOS|\nSTART TESTS|$)', memory_text, re.DOTALL)
        
        for section in smbios_sections:
            module_data = {
                'manufacturer': self._extract_field(section, r'MANUFACTURER:\s*(.+)'),
                'part_number': self._extract_field(section, r'PART_NUMBER:\s*(.+)'),
                'serial_number': self._extract_field(section, r'SERIAL_NUMBER:\s*(.+)'),
                'capacity': self._extract_numeric(section, r'SIZE:\s*(\d+)'),
                'device_locator': self._extract_field(section, r'DEVICE_LOCATOR:\s*(.+)'),
                'bank_locator': self._extract_field(section, r'BANK_LOCATOR:\s*(.+)')
            }
            memory_modules.append(module_data)
        
        return memory_modules
    
    #hdd or ssd information
    def _parse_storage(self, content):
        storage_section = re.search(r'\+\+\+ .+? STORAGE QUICK DIAGNOSTIC .+?\n(.+?)\n--- STORAGE QUICK DIAGNOSTIC', content, re.DOTALL)
        if not storage_section:
            return None
        
        storage_text = storage_section.group(1)
        
        storage_data = {
            'model': self._extract_field(storage_text, r'MODEL_NUMBER:\s*(.+)'),
            'manufacturer': self._extract_field(storage_text, r'MANUFACTURER:\s*(.+)'),
            'serial_number': self._extract_field(storage_text, r'SERIAL_NUMBER:\s*(.+)'),
            'device_type': self._extract_field(storage_text, r'DEVICE_TYPE:\s*(.+)'),
            'size': self._extract_field(storage_text, r'INFORMATION_SIZE:\s*(.+)'),
            'firmware_revision': self._extract_field(storage_text, r'FIRMWARE_REVISION:\s*(.+)'),
            'temperature': self._extract_field(storage_text, r'TEMPERATURE:\s*(.+)')
        }
        
        return storage_data
    
    #MB information
    def _parse_motherboard(self, content):
        mb_section = re.search(r'\+\+\+ .+? MOTHERBOARD .+? DIAGNOSTIC .+?\n(.+?)\n--- MOTHERBOARD .+? DIAGNOSTIC', content, re.DOTALL)
        if not mb_section:
            return None
        
        mb_text = mb_section.group(1)
        
        mb_data = {
            '8s_code': self._extract_field(mb_text, r'8S_CODE:\s*(.+)'),
            'tb_fw_version': self._extract_field(mb_text, r'TB_FW_VERSION:\s*(.+)'),
            'usb_controllers': self._extract_numeric(mb_text, r'NUMBER_USB_HOST_CONTROLLERS:\s*(\d+)'),
            'pci_devices': self._extract_numeric(mb_text, r'NUMBER_PCI:\s*(\d+)'),
            'rtc_presence': self._extract_field(mb_text, r'RTC_PRESENCE:\s*(.+)')
        }
        
        return mb_data
    
    #individual test results
    def _parse_test_results(self, content):
        test_results = []

        #Finds all test sections based on START and STOP markers
        test_sections = re.findall(r'START TESTS\n(.*?)\nSTOP TESTS', content, re.DOTALL)
        
        #extracts individual test results from each section based on timestamps and results
        for section in test_sections:
            tests = re.findall(r'(\d{8}T\d{6}UTC) START (.+?)\n.*?(\d{8}T\d{6}UTC) STOP \2 (PASSED|FAILED|NOT APPLICABLE) (\d+) S', section, re.DOTALL)
            
            for test in tests:
                test_result = {
                    'test_name': test[1],
                    'start_time': datetime.strptime(test[0], '%Y%m%dT%H%M%SUTC'),
                    'end_time': datetime.strptime(test[2], '%Y%m%dT%H%M%SUTC'),
                    'result': test[3],
                    'duration': int(test[4])
                }
                test_results.append(test_result)
        
        return test_results
    
    #creates a summary of the test results
    def _parse_test_summary(self, content):
        summary_section = re.search(r'\+\+\+ TEST SUMMARY\n(.+?)\n--- TEST SUMMARY', content, re.DOTALL)
        if not summary_section:
            return None
        
        summary_text = summary_section.group(1)
        
        summary = {
            'total_tests': self._extract_numeric(summary_text, r'TOTAL_TESTS:\s*(\d+)'),
            'passed_tests': self._extract_numeric(summary_text, r'PASSED_TESTS:\s*(\d+)'),
            'failed_tests': self._extract_numeric(summary_text, r'FAILED_TESTS:\s*(\d+)'),
            'warning_tests': self._extract_numeric(summary_text, r'WARNING_TESTS:\s*(\d+)'),
            'canceled_tests': self._extract_numeric(summary_text, r'CANCELED_TESTS:\s*(\d+)'),
            'not_applicable_tests': self._extract_numeric(summary_text, r'NOT_APPLICABLE_TESTS:\s*(\d+)'),
            'elapsed_time': self._extract_field(summary_text, r'ELAPSED_TIME:\s*(.+)'),
            'final_result_code': self._extract_field(summary_text, r'FINAL_RESULT_CODE:\s*(.+)')
        }
        
        return summary
    

    def _extract_numeric(self, text, pattern):
        match = re.search(pattern, text)
        return int(match.group(1)) if match else None
    
    #validates the battery health and cycle count based on Lenovo standards (original method)
    def _validate_battery(self, health_percentage, cycles):
        status = "PASSED"
        messages = []
        
        # Check battery health percentage (must be >= 80%)
        if health_percentage is not None:
            if health_percentage < 80.0:
                status = "FAILED"
                messages.append(f"Battery health {health_percentage:.1f}% is below 80%")
        else:
            messages.append("Could not calculate battery health percentage")
        
        # Check cycle count (must be < 750)
        if cycles is not None:
            if cycles >= 750:
                status = "FAILED"
                messages.append(f"Battery cycles {cycles} exceeds limit of 750")
        else:
            messages.append("Could not determine battery cycle count")
        
        if status == "PASSED" and not messages:
            messages.append("Battery validation passed")
        elif status == "PASSED" and messages:
            # Has warnings but still passed
            pass
        
        return {
            'status': status,
            'message': '; '.join(messages)
        }
    
    #validates the battery health and cycle count based on updated Lenovo standards (from process_real_data.py)
    def _validate_battery_new(self, health_percentage, cycles):
        """Validate battery based on Lenovo standards"""
        if health_percentage is None or cycles is None:
            return {'status': 'UNKNOWN', 'message': 'Insufficient data for validation'}
        
        if health_percentage >= 80 and cycles <= 500:
            return {'status': 'GOOD', 'message': 'Battery health is within acceptable range'}
        elif health_percentage >= 70 and cycles <= 800:
            return {'status': 'FAIR', 'message': 'Battery health is fair, consider replacement soon'}
        else:
            return {'status': 'POOR', 'message': 'Battery health is poor, replacement recommended'}
    
    # JSON-specific parsing methods (updated from process_real_data.py)
    def _parse_json_battery(self, modules):
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
                    
                    # Validate battery using updated validation logic
                    validation = self._validate_battery_new(health_percentage, cycles_num)
                    
                    return {
                        'serial_number': details.get('SERIAL_NUMBER'),
                        'manufacturer': details.get('MANUFACTURER'),
                        'design_capacity': design_capacity_num,
                        'full_charge_capacity': full_charge_capacity_num,
                        'cycles': cycles_num,
                        'health_percentage': health_percentage,
                        'validation_status': validation['status'],
                        'validation_message': validation['message']
                    }
        return None
    
    def _parse_json_display(self, modules):
        """Parse display information from JSON modules"""
        for module in modules:
            if module.get('name') == 'DISPLAY':
                diagnostics = module.get('diagnostics', [])
                if diagnostics:
                    diag = diagnostics[0]
                    properties = diag.get('properties', {})
                    
                    # Parse resolution
                    native_res = properties.get('NATIVE_RESOLUTION', '0x0')
                    width, height = 0, 0
                    if 'x' in native_res:
                        try:
                            width, height = map(int, native_res.split('x'))
                        except ValueError:
                            pass
                    
                    return {
                        'name': diag.get('udi', '').split(' - ')[-1] if diag.get('udi') else None,
                        'serial_number': diag.get('udi'),
                        'manufacturer_id': properties.get('MANUFACTURER_ID'),
                        'manufacturer_code': properties.get('MANUFACTURER_ID'),
                        'width': width,
                        'height': height,
                        'max_nits': 300.0,
                        'min_nits': 0.5
                    }
        return None
    
    def _parse_json_cpu(self, modules):
        """Parse CPU information from JSON modules"""
        for module in modules:
            if module.get('name') == 'CPU':
                diagnostics = module.get('diagnostics', [])
                if diagnostics:
                    diag = diagnostics[0]
                    properties = diag.get('properties', {})
                    
                    return {
                        'model': properties.get('CPU_MODEL'),
                        'vendor': properties.get('CPU_VENDOR'),
                        'cores': int(properties.get('CPU_CORES', 0)) if properties.get('CPU_CORES') else None,
                        'threads': int(properties.get('CPU_THREADS', 0)) if properties.get('CPU_THREADS') else None,
                        'max_speed': properties.get('CPU_CURRENT_SPEED'),
                        'features': properties.get('CPU_FEATURES')
                    }
        return None
    
    def _parse_json_memory(self, modules):
        """Parse memory information from JSON modules (updated from process_real_data.py)"""
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
                    
                    # Return single memory entry with summary
                    return [{
                        'manufacturer': sample_module.get('MANUFACTURER', 'Unknown'),
                        'part_number': sample_module.get('PART_NUMBER', 'Unknown'),
                        'serial_number': f'Module_Count_{module_count}',
                        'capacity': int(total_memory_gb * 1024) if total_memory_gb > 0 else 0,  # Convert to MB
                        'device_locator': f'TOTAL_{module_count}_MODULES',
                        'bank_locator': f'{sample_module.get("TYPE", "Unknown")}_MEMORY'
                    }]
        return []
    
    def _parse_json_storage(self, modules):
        """Parse storage information from JSON modules (updated from process_real_data.py)"""
        for module in modules:
            if module.get('name') == 'STORAGE':
                diagnostics = module.get('diagnostics', [])
                if diagnostics:
                    diag = diagnostics[0]
                    details = diag.get('properties', {})
                    
                    return {
                        'model': details.get('MODEL'),
                        'manufacturer': details.get('MODEL', '').split(' ')[0] if details.get('MODEL') else None,
                        'serial_number': details.get('SERIAL'),
                        'device_type': details.get('PROTOCOL', 'Unknown'),
                        'size': details.get('SIZE'),
                        'firmware_revision': details.get('FIRMWARE'),
                        'temperature': details.get('TEMPERATURE')
                    }
        return None
    
    def _parse_json_motherboard(self, modules):
        """Parse motherboard information from JSON modules"""
        for module in modules:
            if module.get('name') == 'MOTHERBOARD':
                diagnostics = module.get('diagnostics', [])
                if diagnostics:
                    diag = diagnostics[0]
                    properties = diag.get('properties', {})
                    
                    return {
                        '8s_code': None,  # Not available in JSON format
                        'tb_fw_version': None,  # Not available in JSON format
                        'usb_controllers': int(properties.get('MOTHERBOARD_USB_HOST_CONTROLLER_COUNT', 0)) if properties.get('MOTHERBOARD_USB_HOST_CONTROLLER_COUNT') else None,
                        'pci_devices': int(properties.get('MOTHERBOARD_PCI_DEVICE_COUNT', 0)) if properties.get('MOTHERBOARD_PCI_DEVICE_COUNT') else None,
                        'rtc_presence': properties.get('MOTHERBOARD_RTC_PRESENT')
                    }
        return None
    
    def _parse_json_test_results(self, modules):
        """Parse individual test results from JSON modules"""
        test_results = []
        
        for module in modules:
            module_name = module.get('name', '')
            diagnostics = module.get('diagnostics', [])
            
            for diag in diagnostics:
                tests = diag.get('tests', [])
                
                for test in tests:
                    start_time = self._parse_json_timestamp(test.get('start_time'))
                    finish_time = self._parse_json_timestamp(test.get('finish_time'))
                    
                    test_result = {
                        'component': module_name,
                        'test_name': test.get('name'),
                        'start_time': start_time,
                        'end_time': finish_time,
                        'duration': int(test.get('duration', 0)) if test.get('duration') else 0,
                        'status': 'PASSED' if test.get('result') == 'SUCCESS' else 'FAILED'
                    }
                    test_results.append(test_result)
        
        return test_results
    
    def _parse_json_test_summary(self, iteration):
        """Parse test summary from JSON iteration"""
        overall_status = iteration.get('overall_status')
        
        # Count tests from all modules
        total_tests = 0
        passed_tests = 0
        failed_tests = 0
        
        modules = iteration.get('modules', [])
        for module in modules:
            diagnostics = module.get('diagnostics', [])
            for diag in diagnostics:
                tests = diag.get('tests', [])
                for test in tests:
                    total_tests += 1
                    if test.get('result') == 'SUCCESS':
                        passed_tests += 1
                    else:
                        failed_tests += 1
        
        # Calculate elapsed time
        start_time = self._parse_json_timestamp(iteration.get('start_time'))
        finish_time = self._parse_json_timestamp(iteration.get('finish_time'))
        elapsed_time = None
        if start_time and finish_time:
            elapsed_seconds = (finish_time - start_time).total_seconds()
            elapsed_time = f"{int(elapsed_seconds)} S"
        
        return {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'warning_tests': 0,  # Not tracked in JSON format
            'canceled_tests': 0,  # Not tracked in JSON format
            'not_applicable_tests': 0,  # Not tracked in JSON format
            'elapsed_time': elapsed_time,
            'final_result_code': iteration.get('final_result_code')
        }
    
    def _extract_capacity_value(self, capacity_str):
        """Extract numeric capacity value from strings like '50270mWh (3059mAh)'"""
        if not capacity_str:
            return None
        
        # Extract the first numeric value (mWh)
        import re
        match = re.search(r'(\d+)mWh', capacity_str)
        if match:
            return int(match.group(1))
        
        # Fallback to any numeric value
        match = re.search(r'(\d+)', capacity_str)
        if match:
            return int(match.group(1))
        
        return None
    
    #uploads the information to the new Lenovo diagnostics database
    #and returns the system_id of the uploaded data
    def upload_to_database(self, data, employee_number=None):
        if not self.conn:
            raise Exception("Database connection not established")
        
        cursor = self.conn.cursor()
        
        try:
            # 1. Insert/Update system_info (main table)
            cursor.execute("""
                INSERT INTO system_info (serial_number, machine_model, machine_type_model, bios_version, app_version, execution_type, start_time, finish_time) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (serial_number) DO UPDATE SET 
                    machine_model = EXCLUDED.machine_model,
                    machine_type_model = EXCLUDED.machine_type_model,
                    bios_version = EXCLUDED.bios_version,
                    app_version = EXCLUDED.app_version,
                    execution_type = EXCLUDED.execution_type,
                    start_time = EXCLUDED.start_time,
                    finish_time = EXCLUDED.finish_time,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                data['serial_number'],
                data['machine_model'],
                data['machine_type_model'],
                data['bios_version'],
                data['app_version'],
                data['execution_type'],
                data.get('start_time'),
                data.get('finish_time')
            ))
            
            system_id = cursor.fetchone()[0]
            
            # 2. Insert battery if available
            if data['battery']:
                battery = data['battery']
                # Delete existing battery record for this system first
                cursor.execute("DELETE FROM battery WHERE system_id = %s", (system_id,))
                
                cursor.execute("""
                    INSERT INTO battery (system_id, serial_number, manufacturer, design_capacity, full_charge_capacity, cycles, health_percentage, validation_status, validation_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    system_id,
                    battery['serial_number'],
                    battery['manufacturer'],
                    f"{battery['design_capacity']} mWh" if battery['design_capacity'] else None,
                    f"{battery['full_charge_capacity']} mWh" if battery['full_charge_capacity'] else None,
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
                # Delete existing display record for this system first
                cursor.execute("DELETE FROM display WHERE system_id = %s", (system_id,))
                
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
                # Delete existing CPU record for this system first
                cursor.execute("DELETE FROM cpu WHERE system_id = %s", (system_id,))
                
                cursor.execute("""
                    INSERT INTO cpu (system_id, model, manufacturer, cores, threads, current_speed, cache_l1, cache_l2, cache_l3)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    system_id,
                    cpu.get('model'),
                    cpu.get('manufacturer'),  # Use manufacturer instead of vendor
                    cpu.get('cores'),
                    cpu.get('threads'),
                    cpu.get('current_speed'),  # Use current_speed instead of max_speed
                    cpu.get('cache_l1'),  # Use actual cache_l1 instead of features
                    cpu.get('cache_l2'),  # Use actual cache_l2
                    cpu.get('cache_l3')   # Use actual cache_l3
                ))
            
            # 5. Insert memory (single entry with module count)
            if data['memory']:
                # Delete existing memory records for this system first
                cursor.execute("DELETE FROM memory WHERE system_id = %s", (system_id,))
                
                for memory in data['memory']:
                    if memory['capacity']:
                        cursor.execute("""
                            INSERT INTO memory (system_id, total_memory, module_count, module_type, module_manufacturer, module_size, module_speed, module_part_number)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            system_id,
                            f"{memory['capacity'] / 1024:.0f} GB",  # Convert MB to GB
                            1,  # Placeholder for module count
                            memory['bank_locator'],
                            memory['manufacturer'],
                            f"{memory['capacity']} MB",
                            'Unknown',  # Speed not available in current format
                            memory['part_number']
                        ))
                        break  # Only insert first entry since we're using summary format
            
            # 6. Insert storage if available
            if data['storage']:
                # Delete existing storage records for this system first
                cursor.execute("DELETE FROM storage WHERE system_id = %s", (system_id,))
                
                storage = data['storage']
                cursor.execute("""
                    INSERT INTO storage (system_id, model, serial_number, size, protocol, firmware, temperature)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    system_id,
                    storage.get('model'),
                    storage.get('serial_number'),
                    storage.get('size'),
                    storage.get('device_type'),
                    storage.get('firmware_revision'),
                    storage.get('temperature')
                ))
            
            # 7. Insert motherboard if available
            if data['motherboard']:
                # Delete existing motherboard records for this system first
                cursor.execute("DELETE FROM motherboard WHERE system_id = %s", (system_id,))
                
                mb = data['motherboard']
                cursor.execute("""
                    INSERT INTO motherboard (system_id, usb_controllers, pci_devices, rtc_present)
                    VALUES (%s, %s, %s, %s)
                """, (
                    system_id,
                    mb.get('usb_controllers'),
                    mb.get('pci_devices'),
                    mb.get('rtc_presence')
                ))
            
            # 8. Insert individual test results
            if data['test_results']:
                # Clear existing test results for this system
                cursor.execute("DELETE FROM test_results WHERE system_id = %s", (system_id,))
                
                for test in data['test_results']:
                    test_passed = test.get('status') == 'PASSED' or test.get('result') == 'PASSED'
                    test_name = test.get('test_name') or f"{test.get('component', 'UNKNOWN')} - {test.get('test_name', 'UNKNOWN')}"
                    
                    cursor.execute("""
                        INSERT INTO test_results (system_id, test_name, result, passed)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        system_id,
                        test_name,
                        test.get('status') or test.get('result', 'UNKNOWN'),
                        test_passed
                    ))
            
            self.conn.commit()
            logger.info(f"Successfully uploaded data for device {data['serial_number']}, system_id: {system_id}")
            return system_id
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error uploading to database: {e}")
            raise
        finally:
            cursor.close()

    #batch processing of JSON files in a folder
    def process_json_folder(self, folder_path, employee_number=None, file_pattern="*.json"):
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")
        
        # Find all JSON files matching the pattern
        json_files = glob.glob(os.path.join(folder_path, file_pattern))
        
        if not json_files:
            logger.warning(f"No JSON files found in {folder_path} matching pattern {file_pattern}")
            return []
        
        logger.info(f"Found {len(json_files)} JSON files to process")
        
        results = []
        successful_uploads = 0
        failed_uploads = 0
        
        for json_file in json_files:
            try:
                logger.info(f"Processing file: {os.path.basename(json_file)}")
                
                # Parse the JSON file
                data = self.parse_json_file(json_file)
                
                # Upload to database
                system_id = self.upload_to_database(data, employee_number)
                
                results.append({
                    'file': os.path.basename(json_file),
                    'status': 'success',
                    'system_id': system_id,
                    'serial_number': data.get('serial_number', 'Unknown')
                })
                successful_uploads += 1
                
            except Exception as e:
                logger.error(f"Failed to process {os.path.basename(json_file)}: {e}")
                results.append({
                    'file': os.path.basename(json_file),
                    'status': 'failed',
                    'error': str(e),
                    'system_id': None,
                    'serial_number': 'Unknown'
                })
                failed_uploads += 1
        
        logger.info(f"Batch processing completed: {successful_uploads} successful, {failed_uploads} failed")
        return results

    #Generates report of the processed files
    def generate_batch_report(self, results, output_file=None):
        report = {
            'summary': {
                'total_files': len(results),
                'successful': len([r for r in results if r['status'] == 'success']),
                'failed': len([r for r in results if r['status'] == 'failed']),
                'processed_at': datetime.now().isoformat()
            },
            'details': results
        }
        
        if output_file:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            reports_dir = os.path.join(script_dir, "Reports")
            
            if not os.path.exists(reports_dir):
                os.makedirs(reports_dir)
                logger.info(f"Created Reports directory: {reports_dir}")
            
            if not os.path.dirname(output_file):
                output_file = os.path.join(reports_dir, output_file)
            
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Batch report saved to: {output_file}")
        
        return report

def main():
    try:
        # Load database configuration from config file
        db_config = load_db_config()
        parser = DiagnosticLogParser(db_config)
        
        # Connect to database
        parser.connect_db()
        
        # BATCH PROCESSING - Process entire folder with JSON files
        # folder_path = r"c:\Users\eduardo.beltran\Documents\Lenovo\self test log upload"
        folder_path = r"D:\Log"
        employee_number = '21883'
        
        logger.info(f"Starting batch processing of JSON files in folder: {folder_path}")
        
        #process the JSON files in the folder
        results = parser.process_json_folder(folder_path, employee_number, "*.json")
        #adds today date and the report name
        today = datetime.now().strftime("%Y%m%d")
        report = parser.generate_batch_report(results, f'processed_report_{today}.json')

        #Summary of the processed files
        print(f"\n=== BATCH PROCESSING SUMMARY ===")
        print(f"Total files: {report['summary']['total_files']}")
        print(f"Successful: {report['summary']['successful']}")
        print(f"Failed: {report['summary']['failed']}")
        
        if report['summary']['failed'] > 0:
            print(f"\nFailed files:")
            for result in results:
                if result['status'] == 'failed':
                    print(f"  - {result['file']}: {result['error']}")
        
    except Exception as e:
        logger.error(f"Batch process failed: {e}")
    finally:
        if 'parser' in locals():
            parser.close_db()

if __name__ == "__main__":
    main()