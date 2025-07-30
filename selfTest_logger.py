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
        
        return config_namespace['db_config']
    
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise
    except KeyError:
        logger.error(f"db_config not found in config file: {config_path}")
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
    
    #parses the log file to be able to read and extract the information
    #and returns a dictionary with the parsed data
    def parse_log_file(self, log_file_path):
        with open(log_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        data = {}
        
        #System information
        data['serial_number'] = self._extract_field(content, r'SERIAL_NUMBER:\s*(.+)')
        data['bios_version'] = self._extract_field(content, r'BIOS_VERSION:\s*(.+)')
        data['machine_model'] = self._extract_field(content, r'MACHINE_MODEL:\s*(.+)')
        data['app_version'] = self._extract_field(content, r'APPLICATION_VERSION:\s*(.+)')
        data['execution_type'] = self._extract_field(content, r'EXECUTION_TYPE:\s*(.+)')
        
        #Time for time started
        data['execution_start'] = self._extract_execution_time(content)
        
        #Computer components
        data['battery'] = self._parse_battery(content)
        data['display'] = self._parse_display(content)
        data['cpu'] = self._parse_cpu(content)
        data['memory'] = self._parse_memory(content)
        data['storage'] = self._parse_storage(content)
        data['motherboard'] = self._parse_motherboard(content)
        
        #Test results
        data['test_results'] = self._parse_test_results(content)
        data['test_summary'] = self._parse_test_summary(content)
        
        return data
    
    def _extract_field(self, content, pattern):
        match = re.search(pattern, content)
        return match.group(1).strip() if match else None
    
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
    
    #validates the battery health and cycle count based on Lenovo standards
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
    
    #uploads the information to the database
    #and returns the log_test_id of the uploaded data
    def upload_to_database(self, data, employee_number=None):
        if not self.conn:
            raise Exception("Database connection not established")
        
        cursor = self.conn.cursor()
        
        try:
            # 1. Insert/Update device
            device_id = data['serial_number']
            machine_model = data['machine_model']
            
            cursor.execute("""
                INSERT INTO mb_api_device ("idSerial", "partNo") 
                VALUES (%s, %s) 
                ON CONFLICT ("idSerial") DO UPDATE SET "partNo" = EXCLUDED."partNo"
            """, (device_id, machine_model))
            
            # 2. Insert/Update motherboard
            cursor.execute("""
                INSERT INTO mb_api_motherboard (serial_number, bios_version, manufacturer, "original_productKey", product_id, "SKU")
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (serial_number) DO UPDATE SET
                    bios_version = EXCLUDED.bios_version,
                    manufacturer = EXCLUDED.manufacturer
            """, (
                data['serial_number'],
                data['bios_version'],
                'Lenovo',
                '',
                machine_model,
                ''
            ))
            
            # 3. Insert battery if available
            battery_id = None
            if data['battery']:
                cursor.execute("""
                    INSERT INTO mb_api_battery (serial_number, manufacture_name, desing_capacity, full_charge_capacity, device_name, cycles)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    data['battery']['serial_number'],
                    data['battery']['manufacturer'],
                    data['battery']['design_capacity'] or 0,
                    data['battery']['full_charge_capacity'] or 0,
                    data['battery']['device_name'],
                    data['battery']['cycles']
                ))
                battery_id = cursor.fetchone()[0]
                
                #Battery results
                battery_status = data['battery']['validation_status']
                battery_message = data['battery']['validation_message']
                logger.info(f"Battery validation - Serial: {data['battery']['serial_number']}, "
                           f"Status: {battery_status}, Health: {data['battery'].get('health_percentage', 'N/A')}%, "
                           f"Cycles: {data['battery'].get('cycles_numeric', 'N/A')}, Message: {battery_message}")
                
                if battery_status == "FAILED":
                    battery_test_result = {
                        'test_name': 'BATTERY_VALIDATION',
                        'start_time': data['execution_start'] or datetime.now(),
                        'end_time': data['execution_start'] or datetime.now(),
                        'result': 'FAILED',
                        'duration': 0,
                        'detail_message': battery_message
                    }
                    if data['test_results'] is None:
                        data['test_results'] = []
                    data['test_results'].append(battery_test_result)
            
            # 4. Insert monitor if available
            monitor_id = None
            if data['display']:
                cursor.execute("""
                    INSERT INTO mb_api_monitor (name, serial_number, max_nits, min_nits, width, height, manufacturer_id, manufacturer_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    data['display']['name'],
                    data['display']['serial_number'],
                    data['display']['max_nits'],
                    data['display']['min_nits'],
                    data['display']['width'],
                    data['display']['height'],
                    data['display']['manufacturer_id'],
                    data['display']['manufacturer_code']
                ))
                monitor_id = cursor.fetchone()[0]
            
            # 5. Insert main test log
            # Check if battery validation failed to update overall test status
            battery_failed = False
            if data['battery'] and data['battery']['validation_status'] == 'FAILED':
                battery_failed = True
            
            # Determine overall test status
            summary_failed = data['test_summary']['failed_tests'] > 0 if data['test_summary'] else False
            all_tests_passed = not (summary_failed or battery_failed)
            
            cursor.execute("""
                INSERT INTO mb_api_logtest (
                    all_tests_passed, test_date, numero_de_empleado, motherboard_id, 
                    mb_type_test, report, when_started, app_ver, monitor_id, battery_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING log_id
            """, (
                all_tests_passed,
                data['execution_start'] or datetime.now(),
                employee_number,
                data['serial_number'],
                data['execution_type'],
                json.dumps(data['test_summary']) if data['test_summary'] else '{}',
                data['execution_start'],
                data['app_version'],
                monitor_id,
                battery_id
            ))
            
            log_test_id = cursor.fetchone()[0]
            
            # 6. Insert individual test results
            if data['test_results']:
                for test in data['test_results']:
                    test_passed = test['result'] == 'PASSED'
                    # Use custom detail message if available, otherwise use duration
                    detail_message = test.get('detail_message', f"Duration: {test['duration']}s")
                    cursor.execute("""
                        INSERT INTO mb_api_individualtestresult (test_name, test_passed, detail_message, test_result_id)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        test['test_name'],
                        test_passed,
                        detail_message,
                        log_test_id
                    ))
            
            # 7. Insert memory modules
            if data['memory']:
                for memory in data['memory']:
                    if memory['capacity']:
                        cursor.execute("""
                            INSERT INTO mb_api_ram (manufacturer, part_number, serial_number, capacity, log_test_id)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            memory['manufacturer'],
                            memory['part_number'],
                            memory['serial_number'] or 'Unknown',
                            memory['capacity'],
                            log_test_id
                        ))
            
            # 8. Insert storage/disk information
            if data['storage']:
                partitions_info = json.dumps({
                    "size": data['storage']['size'],
                    "type": data['storage']['device_type']
                })
                
                cursor.execute("""
                    INSERT INTO mb_api_disk (model, size, serial_number, log_test_id, partitions_info, unallocated)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    data['storage']['model'],
                    data['storage']['size'],
                    data['storage']['serial_number'],
                    log_test_id,
                    partitions_info,
                    '0 GB'
                ))
            
            self.conn.commit()
            logger.info(f"Successfully uploaded data for device {device_id}, log_test_id: {log_test_id}")
            return log_test_id
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error uploading to database: {e}")
            raise
        finally:
            cursor.close()

    #batch processing of log files in a folder
    def process_log_folder(self, folder_path, employee_number=None, file_pattern="*.log"):
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")
        
        # Find all log files matching the pattern
        log_files = glob.glob(os.path.join(folder_path, file_pattern))
        
        if not log_files:
            logger.warning(f"No log files found in {folder_path} matching pattern {file_pattern}")
            return []
        
        logger.info(f"Found {len(log_files)} log files to process")
        
        results = []
        successful_uploads = 0
        failed_uploads = 0
        
        for log_file in log_files:
            try:
                logger.info(f"Processing file: {os.path.basename(log_file)}")
                
                # Parse the log file
                data = self.parse_log_file(log_file)
                
                # Upload to database
                log_test_id = self.upload_to_database(data, employee_number)
                
                results.append({
                    'file': os.path.basename(log_file),
                    'status': 'success',
                    'log_test_id': log_test_id,
                    'serial_number': data.get('serial_number', 'Unknown')
                })
                successful_uploads += 1
                
            except Exception as e:
                logger.error(f"Failed to process {os.path.basename(log_file)}: {e}")
                results.append({
                    'file': os.path.basename(log_file),
                    'status': 'failed',
                    'error': str(e),
                    'log_test_id': None,
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
        
        # BATCH PROCESSING - Process entire folder
        folder_path = r"D:\\Log"
        employee_number = '21883'
        
        logger.info(f"Starting batch processing of folder: {folder_path}")
        
        #process the log files in the folder
        results = parser.process_log_folder(folder_path, employee_number, "*.log")
        report = parser.generate_batch_report(results, 'batch_processing_report.json')
        
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