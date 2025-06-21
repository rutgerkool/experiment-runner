from EventManager.Models.RunnerEvents import RunnerEvents
from EventManager.EventSubscriptionController import EventSubscriptionController
from ConfigValidator.Config.Models.RunTableModel import RunTableModel
from ConfigValidator.Config.Models.FactorModel import FactorModel
from ConfigValidator.Config.Models.RunnerContext import RunnerContext
from ConfigValidator.Config.Models.OperationType import OperationType
from ProgressManager.Output.OutputProcedure import OutputProcedure as output

from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from os.path import dirname, realpath
import os
import time
import datetime
import subprocess
import paramiko
import pandas as pd
import csv
from dotenv import load_dotenv
import threading
import psutil


class PowerLogger:
    def __init__(self, run_dir: Path, vm_qemu_id: str, sample_interval: float = 0.1):
        self.run_dir = run_dir
        self.vm_qemu_id = vm_qemu_id
        self.sample_interval = sample_interval
        self.qemu_pid = None
        self.stop_flag = False
        self.power_data = []
        self.power_log_file = None
        self.power_log_thread = None
        self.powerjoular_proc = None
        self.start_time = None
        
    def get_qemu_pid(self) -> Optional[int]:
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                cmdline = " ".join(proc.info['cmdline'])
                if f"-id {self.vm_qemu_id}" in cmdline:
                    return proc.info['pid']
            except Exception:
                continue
        return None
        
    def read_rapl_energy(self) -> Tuple[int, int]:
        try:
            with open("/sys/class/powercap/intel-rapl:0/energy_uj") as f:
                pkg = int(f.read())
            with open("/sys/class/powercap/intel-rapl:0:0/energy_uj") as f:
                dram = int(f.read())
            return pkg, dram
        except:
            return 0, 0
            
    def read_proc_io(self, pid: int) -> Tuple[int, int]:
        try:
            with open(f"/proc/{pid}/io", "r") as f:
                content = f.read()
                
            read_bytes = 0
            write_bytes = 0
            
            for line in content.strip().split('\n'):
                parts = line.split(':')
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = int(parts[1].strip())
                    
                    if key == "read_bytes":
                        read_bytes = value
                    elif key == "write_bytes":
                        write_bytes = value
                        
            return read_bytes, write_bytes
        except Exception as e:
            output.console_log(f"Error reading /proc/{pid}/io: {str(e)}")
            return 0, 0
            
    def start_powerjoular(self, pid: int) -> subprocess.Popen:
        output_file = self.run_dir / f"powerjoular_{pid}.csv"
        output.console_log(f"Starting PowerJoular for PID {pid} → {output_file}")
        return subprocess.Popen([
            "powerjoular",
            '-p', str(pid),
            '-f', str(output_file)
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    def collect_vm_process_metrics(self, pid: int) -> Dict[str, Any]:
        try:
            metrics = {}
            
            process = psutil.Process(pid)
            
            metrics['cpu_percent'] = process.cpu_percent(interval=0.1)
            
            memory_info = process.memory_info()
            metrics['memory_rss_mb'] = memory_info.rss / (1024 * 1024)
            metrics['memory_vms_mb'] = memory_info.vms / (1024 * 1024)
            
            metrics['thread_count'] = process.num_threads()
            
            try:
                io_counters = process.io_counters()
                metrics['io_read_count'] = io_counters.read_count
                metrics['io_write_count'] = io_counters.write_count
                metrics['io_read_bytes_total'] = io_counters.read_bytes
                metrics['io_write_bytes_total'] = io_counters.write_bytes
            except (psutil.AccessDenied, AttributeError):
                pass
                
            return metrics
        except Exception as e:
            output.console_log(f"Error collecting VM process metrics: {str(e)}")
            return {}
        
    def log_power_measurements(self, log_file, last_pkg, last_dram, last_read, last_write, last_time):
        now = time.time()
        delta = now - last_time

        pkg, dram = self.read_rapl_energy()
        read_bytes, write_bytes = self.read_proc_io(self.qemu_pid)

        cpu_power = (pkg - last_pkg) / (delta * 1e6)
        dram_power = (dram - last_dram) / (delta * 1e6)
        delta_read = read_bytes - last_read
        delta_write = write_bytes - last_write

        vm_metrics = self.collect_vm_process_metrics(self.qemu_pid)
        
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row_data = {
            "timestamp": ts,
            "cpu_power_w": cpu_power,
            "dram_power_w": dram_power,
            "read_bytes": delta_read,
            "write_bytes": delta_write
        }
        
        row_data.update(vm_metrics)
        
        self.power_data.append(row_data)
        
        csv_parts = [ts, f"{cpu_power:.3f}", f"{dram_power:.3f}", str(delta_read), str(delta_write)]
        
        for key, value in vm_metrics.items():
            if isinstance(value, float):
                csv_parts.append(f"{value:.2f}")
            else:
                csv_parts.append(str(value))
        
        row_csv = ",".join(csv_parts)
        log_file.write(row_csv + "\n")
        log_file.flush()
        
        return pkg, dram, read_bytes, write_bytes, now
        
    def power_logging_thread(self):
        self.qemu_pid = self.get_qemu_pid()
        if not self.qemu_pid:
            output.console_log_FAIL(f"Could not find QEMU PID for VMID {self.vm_qemu_id}")
            return

        self.power_log_file = self.run_dir / "power_log.csv"
        
        output.console_log(f"Logging RAPL + VM process metrics for VM {self.vm_qemu_id} (PID {self.qemu_pid})")
        output.console_log(f"Output file: {self.power_log_file}")
        
        self.powerjoular_proc = self.start_powerjoular(self.qemu_pid)
        
        sample_metrics = self.collect_vm_process_metrics(self.qemu_pid)
        
        header_parts = ["timestamp", "cpu_power_w", "dram_power_w", "read_bytes", "write_bytes"]
        header_parts.extend(sample_metrics.keys())
        header = ",".join(header_parts)
        
        last_pkg, last_dram = self.read_rapl_energy()
        last_read, last_write = self.read_proc_io(self.qemu_pid)
        last_time = time.time()
        self.start_time = time.time()

        with open(self.power_log_file, 'w') as log:
            log.write(header + "\n")
            
            while not self.stop_flag:
                time.sleep(self.sample_interval)
                last_pkg, last_dram, last_read, last_write, last_time = self.log_power_measurements(
                    log, last_pkg, last_dram, last_read, last_write, last_time
                )
                
    def start(self):
        self.power_log_thread = threading.Thread(target=self.power_logging_thread)
        self.power_log_thread.daemon = True
        self.power_log_thread.start()
        
    def stop(self):
        self.stop_flag = True
        
        if self.power_log_thread:
            self.power_log_thread.join(timeout=10)
            
        if self.powerjoular_proc:
            try:
                self.powerjoular_proc.terminate()
                self.powerjoular_proc.wait(timeout=5)
            except:
                self.powerjoular_proc.kill()
                
        return self.power_data


class ExternalMachineAPI:
    
    def __init__(self, hostname=None, username=None, password=None):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.stdin = None
        self.stdout = None
        self.stderr = None
        
        try:
            host = hostname or os.getenv('HOSTNAME')
            user = username or os.getenv('USERNAME')
            pwd = password or os.getenv('PASSWORD')
            
            self.ssh.connect(hostname=host, username=user, password=pwd)
            output.console_log_OK(f'Connected to machine: {host}')
        except paramiko.SSHException as e:
            output.console_log_FAIL(f'Failed to connect to machine: {str(e)}')
            raise
            
    def _process_command_output(self, wait_for_output, log_file_handle):
        all_output = ""
        
        while True:
            if self.stdout.channel.recv_ready():
                chunk = self.stdout.channel.recv(4096).decode('utf-8', errors='replace')
                all_output += chunk
                
                if log_file_handle:
                    log_file_handle.write(chunk)
                    log_file_handle.flush()
                else:
                    output.console_log(chunk.strip())
                
                if wait_for_output and wait_for_output in all_output:
                    output.console_log_OK(f"Found expected output: '{wait_for_output}'")
                    return True, all_output
            
            time.sleep(0.5)
            
            if self.stdout.channel.exit_status_ready():
                break
        
        return False, all_output

    def execute_remote_command(self, command: str = '', overwrite_channels: bool = True, 
                              wait_for_output: str = None, 
                              log_file: Path = None) -> bool:
        try:
            output.console_log(f"Executing command: {command}")
            
            if overwrite_channels:
                self.stdin, self.stdout, self.stderr = self.ssh.exec_command(command)
            else:
                self.ssh.exec_command(command)
                
            if not (wait_for_output or log_file):
                return True
                
            log_file_handle = open(log_file, 'w', encoding='utf-8') if log_file else None
            
            found_output, all_output = self._process_command_output(
                wait_for_output, log_file_handle
            )
            
            if found_output and log_file_handle:
                log_file_handle.close()
                return True
                
            remainder = self.stdout.read().decode('utf-8', errors='replace')
            if remainder:
                all_output += remainder
                if log_file_handle:
                    log_file_handle.write(remainder)
                else:
                    output.console_log(remainder.strip())
            
            if log_file_handle:
                log_file_handle.close()
                
            if wait_for_output:
                if wait_for_output in all_output:
                    output.console_log_OK(f"Found expected output: '{wait_for_output}'")
                    return True
                else:
                    output.console_log_FAIL(f"Expected output not found: '{wait_for_output}'")
                    return False
            
            return True
            
        except paramiko.SSHException as e:
            output.console_log_FAIL(f'Failed to send command to machine: {str(e)}')
            return False
        except TimeoutError:
            output.console_log_FAIL('Timeout reached while waiting for command output.')
            return False

    def copy_file_from_remote(self, remote_path, local_path):
        try:
            output.console_log(f"Copying {remote_path} to {local_path}")
            sftp = self.ssh.open_sftp()
            sftp.get(remote_path, local_path)
            sftp.close()
            output.console_log_OK(f"Successfully copied {remote_path} to {local_path}")
            return True
        except Exception as e:
            output.console_log_FAIL(f"Failed to copy file: {str(e)}")
            return False

    def __del__(self):
        if hasattr(self, 'stdin') and self.stdin:
            self.stdin.close()
        if hasattr(self, 'stdout') and self.stdout:
            self.stdout.close()
        if hasattr(self, 'stderr') and self.stderr:
            self.stderr.close()
        if hasattr(self, 'ssh') and self.ssh:
            self.ssh.close()


class ProcessManager:
    
    def __init__(self, vm_api: ExternalMachineAPI):
        self.vm_api = vm_api
        
    def check_and_kill_processes(self, process_names: List[str]) -> bool:
        process_name_list = "','".join(process_names)
        self.vm_api.execute_remote_command(
            f"powershell -Command \"Get-Process -Name '{process_name_list}' -ErrorAction SilentlyContinue\""
        )
        
        output_text = self.vm_api.stdout.read().decode('utf-8', errors='replace')
        if output_text.strip():
            output.console_log_FAIL(f"Processes {process_names} still running, killing them forcefully")
            self.vm_api.execute_remote_command(
                f"powershell -Command \"Get-Process -Name '{process_name_list}' | ForEach-Object {{ $_.Kill() }}\""
            )
            return True
        return False


class RunnerConfig:
    ROOT_DIR = Path(dirname(realpath(__file__)))

    name:                       str             = "SB"
    results_output_path:        Path            = ROOT_DIR / 'experiments'
    operation_type:             OperationType   = OperationType.AUTO
    time_between_runs_in_ms:    int             = 120000

    app_vm_host:                str             = None
    test_vm_host:               str             = None
    vm_username:                str             = None
    vm_password:                str             = None
    vm_qemu_id:                 str             = None
    app_ready_text:             str             = "Post Deployment actions are finished: ready for import"
    
    def __init__(self):
        self._load_environment_variables()
        self._setup_event_subscriptions()
        
        self.run_table_model = None
        self.power_logger = None
        
        output.console_log_OK(f"AFAS SB energy test config loaded - Using app VM {self.app_vm_host} and test VM {self.test_vm_host}")

    def _load_environment_variables(self):
        load_dotenv()
        
        self.app_vm_host = os.getenv('APP_VM_HOST')
        self.test_vm_host = os.getenv('TEST_VM_HOST')
        self.vm_username = os.getenv('VM_USERNAME')
        self.vm_password = os.getenv('VM_PASSWORD')
        self.vm_qemu_id = os.getenv('VM_QEMU_ID')
        
    def _setup_event_subscriptions(self):
        EventSubscriptionController.subscribe_to_multiple_events([
            (RunnerEvents.BEFORE_EXPERIMENT, self.before_experiment),
            (RunnerEvents.BEFORE_RUN       , self.before_run       ),
            (RunnerEvents.START_RUN        , self.start_run        ),
            (RunnerEvents.START_MEASUREMENT, self.start_measurement),
            (RunnerEvents.INTERACT         , self.interact         ),
            (RunnerEvents.STOP_MEASUREMENT , self.stop_measurement ),
            (RunnerEvents.STOP_RUN         , self.stop_run         ),
            (RunnerEvents.POPULATE_RUN_DATA, self.populate_run_data),
            (RunnerEvents.AFTER_EXPERIMENT , self.after_experiment )
        ])

    def create_run_table_model(self) -> RunTableModel:
        version_factor = FactorModel("app_version", ["current"])
        
        self.run_table_model = RunTableModel(
            factors=[version_factor],
            data_columns=[
                'test_duration',
                'test_success_rate',
                'scenario_count',
                'powerjoular_power',
                'powerjoular_util',
                'vm_avg_cpu_percent',
                'vm_avg_memory_mb',
                'vm_avg_thread_count',
                'vm_total_io_read_count',
                'vm_total_io_write_count'
            ],
            repetitions = 10
        )
        return self.run_table_model
        
    def _connect_to_vms(self) -> Tuple[ExternalMachineAPI, ExternalMachineAPI]:
        app_vm = ExternalMachineAPI(
            hostname=self.app_vm_host,
            username=self.vm_username,
            password=self.vm_password
        )
        app_vm.execute_remote_command("echo Connection test to APP VM successful")
        
        test_vm = ExternalMachineAPI(
            hostname=self.test_vm_host,
            username=self.vm_username,
            password=self.vm_password
        )
        test_vm.execute_remote_command("echo Connection test to TEST VM successful")
        
        return app_vm, test_vm

    def _cleanup_app_vm_processes(self, app_vm: ExternalMachineAPI):
        app_process_manager = ProcessManager(app_vm)
        app_process_manager.check_and_kill_processes(['Afas.Cqrs.Webserver', 'dotnet'])
    
    def _cleanup_test_vm_processes(self, test_vm: ExternalMachineAPI):
        test_process_manager = ProcessManager(test_vm)
        test_process_manager.check_and_kill_processes(['node', 'playwright'])
        test_process_manager.check_and_kill_processes(['chrome', 'msedge', 'firefox'])

    def _check_powerjoular_installed(self) -> bool:
        try:
            subprocess.run(['powerjoular', '--version'], 
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE, 
                          check=False)
            return True
        except (subprocess.SubprocessError, FileNotFoundError):
            return False

    def before_experiment(self) -> None:
        output.console_log("Initializing experiment...")
        
        try:
            app_vm, test_vm = self._connect_to_vms()
            
            has_powerjoular = self._check_powerjoular_installed()
            if not has_powerjoular:
                output.console_log_FAIL("PowerJoular is not installed or not found in PATH. Energy measurements may be incomplete.")
                
            output.console_log("Checking for lingering processes...")
            self._cleanup_app_vm_processes(app_vm)
            self._cleanup_test_vm_processes(test_vm)

            output.console_log_OK("Pre-experiment checks completed successfully")
            
        except Exception as e:
            output.console_log_FAIL(f"Pre-experiment initialization failed: {str(e)}")
            raise

    def before_run(self) -> None:
        output.console_log("Preparing for test run...")

        subprocess.run(["sudo", "qm", "stop", "111"], check=True)

        subprocess.run(["sudo", "sync"], check=True)
        subprocess.run(["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"], check=True)

        output.console_log("Waiting for system cleanup...")
        time.sleep(180)

        subprocess.run(["sudo", "qm", "start", "111"], check=True)

        output.console_log("Waiting for VMs to boot...")
        time.sleep(300)

        output.console_log("Clearing SQL caches...")
        app_vm = ExternalMachineAPI(hostname=self.app_vm_host, username=self.vm_username, password=self.vm_password)
        app_vm.execute_remote_command(
            'sqlcmd -S localhost -U sa -P "YourStrongPassword123!" -Q "DBCC DROPCLEANBUFFERS; DBCC FREEPROCCACHE; DBCC FREESYSTEMCACHE(\'ALL\');"'
        )

        app_vm.execute_remote_command(
            "powershell -Command \"Restart-Service -Name MSSQLSERVER\""
        )

        output.console_log("Clearing OS caches...")
        app_vm.execute_remote_command(
            "powershell -Command \"[System.Diagnostics.Process]::Start('C:\\Windows\\System32\\rundll32.exe', 'advapi32.dll,ProcessIdleTasks')\""
        )

        test_vm = ExternalMachineAPI(hostname=self.test_vm_host, username=self.vm_username, password=self.vm_password)
        test_vm.execute_remote_command(
            "powershell -Command \"[System.Diagnostics.Process]::Start('C:\\Windows\\System32\\rundll32.exe', 'advapi32.dll,ProcessIdleTasks')\""
        )

        output.console_log("Waiting for thermal stabilization...")
        time.sleep(600)

        output.console_log_OK("Run environment ready for testing")
        
    def start_run(self, context: RunnerContext) -> None:
        output.console_log("Starting .NET application on VM...")
        
        app_log_file = context.run_dir / "app_vm_startup.log"
        
        app_vm = ExternalMachineAPI(
            hostname=self.app_vm_host,
            username=self.vm_username,
            password=self.vm_password
        )
        
        ps_command = "powershell -Command \"Set-Location -Path 'C:\\anta\\sb'; .\\run.ps1\""
        
        output.console_log(f"Starting application, output will be redirected to {app_log_file}")
        success = app_vm.execute_remote_command(
            command=ps_command,
            wait_for_output=self.app_ready_text,
            log_file=app_log_file
        )
        
        if not success:
            output.console_log_FAIL("Failed to start .NET application properly")
        else:            
            output.console_log_OK(".NET application started successfully")
            
        if app_log_file.exists():
            output.console_log(f"Application startup log saved to: {app_log_file}")
            output.console_log(f"Log file size: {app_log_file.stat().st_size} bytes")

    def start_measurement(self, context: RunnerContext) -> None:
        output.console_log("Starting energy measurement...")
        
        self.power_logger = PowerLogger(
            run_dir=context.run_dir,
            vm_qemu_id=self.vm_qemu_id
        )
        
        self.power_logger.start()
        
        time.sleep(2)
        output.console_log_OK("Energy measurement started")

    def _run_tests_on_test_vm(self, context: RunnerContext) -> Tuple[bool, Path]:
        test_log_file = context.run_dir / "test_vm_execution.log"
        local_csv_file = context.run_dir / "scenario_summary.csv"
        
        test_vm = ExternalMachineAPI(
            hostname=self.test_vm_host,
            username=self.vm_username,
            password=self.vm_password
        )
        
        ps_command = "powershell -Command \"Set-Location -Path 'C:\\anta\\features'; .\\run.ps1\""
        output.console_log(f"Running tests, output will be redirected to {test_log_file}")
        test_vm.execute_remote_command(command=ps_command, log_file=test_log_file)
        
        output.console_log("Checking if tests completed successfully...")
        
        time.sleep(2)
        
        csv_remote_path = "C:\\anta\\features\\out\\log\\scenario_summary.csv"
        success = test_vm.copy_file_from_remote(csv_remote_path, str(local_csv_file))
        
        return success, test_log_file, local_csv_file

    def _process_test_results(self, success: bool, context: RunnerContext, test_log_file: Path, local_csv_file: Path):
        if not success or not local_csv_file.exists():
            output.console_log_FAIL("Failed to copy scenario summary CSV file")
            return
        
        try:
            with open(local_csv_file, 'r') as f:
                csv_data = f.read()
                
            lines = csv_data.strip().split("\n")
            scenario_count = len(lines) - 1 if csv_data.strip() and len(lines) > 0 else 0
            
            if "description,startTime,endTime,duration,status" in csv_data and scenario_count > 0:
                output.console_log_OK(f"Test execution completed with {scenario_count} scenarios")
            else:
                output.console_log_FAIL("Test execution may have failed - no valid test results found")
            
        except Exception as e:
            output.console_log_FAIL(f"Error processing CSV file: {str(e)}")
        
        if test_log_file.exists():
            output.console_log(f"Test execution log saved to: {test_log_file}")
            output.console_log(f"Log file size: {test_log_file.stat().st_size} bytes")

    def interact(self, context: RunnerContext) -> None:
        output.console_log("Running tests from test VM...")
        
        success, test_log_file, local_csv_file = self._run_tests_on_test_vm(context)
        self._process_test_results(success, context, test_log_file, local_csv_file)

    def stop_measurement(self, context: RunnerContext) -> None:
        output.console_log("Stopping energy measurement...")
        
        if self.power_logger:
            self.power_logger.stop()
            output.console_log_OK("Energy measurement stopped")
        else:
            output.console_log_FAIL("No power logger was running")

    def stop_run(self, context: RunnerContext) -> None:
        output.console_log("Stopping .NET application and test environment...")
        
        app_vm = ExternalMachineAPI(
            hostname=self.app_vm_host,
            username=self.vm_username,
            password=self.vm_password
        )
        self._cleanup_app_vm_processes(app_vm)
        
        test_vm = ExternalMachineAPI(
            hostname=self.test_vm_host,
            username=self.vm_username,
            password=self.vm_password
        )
        self._cleanup_test_vm_processes(test_vm)
            
        output.console_log_OK("Application and test environment stopped")

    def _process_power_log_data(self, context: RunnerContext) -> Dict[str, float]:
        result = {}
        power_log_file = context.run_dir / "power_log.csv"
        
        if not power_log_file.exists():
            output.console_log_FAIL("No power log file found in run directory")
            return {
                'avg_cpu_power': 0,
                'avg_dram_power': 0,
                'total_energy': 0
            }
            
        output.console_log(f"Found power log file: {power_log_file}")
        
        try:
            df = pd.read_csv(power_log_file)
            
            result['avg_cpu_power'] = round(df['cpu_power_w'].mean(), 3)
            result['avg_dram_power'] = round(df['dram_power_w'].mean(), 3)
            
            duration_hours = len(df) * 0.1 / 3600
            cpu_energy = df['cpu_power_w'].sum() * 0.1 / 3600
            dram_energy = df['dram_power_w'].sum() * 0.1 / 3600
            result['total_energy'] = round(cpu_energy + dram_energy, 3)
            
            if 'cpu_percent' in df.columns:
                result['vm_avg_cpu_percent'] = round(df['cpu_percent'].mean(), 2)
                
            if 'memory_rss_mb' in df.columns:
                result['vm_avg_memory_mb'] = round(df['memory_rss_mb'].mean(), 2)
                
            if 'thread_count' in df.columns:
                result['vm_avg_thread_count'] = round(df['thread_count'].mean(), 2)
                
            if 'io_read_count' in df.columns and len(df) > 1:
                result['vm_total_io_read_count'] = int(df['io_read_count'].iloc[-1]) - int(df['io_read_count'].iloc[0])
            
            if 'io_write_count' in df.columns and len(df) > 1:
                result['vm_total_io_write_count'] = int(df['io_write_count'].iloc[-1]) - int(df['io_write_count'].iloc[0])
                
        except Exception as e:
            output.console_log_FAIL(f"Error processing power log: {str(e)}")
            result = {
                'avg_cpu_power': 0,
                'avg_dram_power': 0,
                'total_energy': 0
            }
            
        return result

    def _process_powerjoular_data(self, context: RunnerContext) -> Dict[str, float]:
        result = {
            'powerjoular_power': 0, 
            'powerjoular_util': 0
        }
        
        powerjoular_files = list(context.run_dir.glob("powerjoular_*.csv"))
        if not powerjoular_files:
            output.console_log_FAIL("No PowerJoular files found in run directory")
            return result
            
        try:
            base_powerjoular_file = self._find_primary_powerjoular_file(powerjoular_files)
            output.console_log(f"Found PowerJoular file: {base_powerjoular_file}")
            
            pj_df = pd.read_csv(base_powerjoular_file)
            
            if 'Total Power' in pj_df.columns:
                result['powerjoular_power'] = round(pj_df['Total Power'].mean(), 3)
                output.console_log(f"PowerJoular average power: {result['powerjoular_power']} W")
            elif 'CPU Power' in pj_df.columns:
                result['powerjoular_power'] = round(pj_df['CPU Power'].mean(), 3)
                output.console_log(f"PowerJoular average power: {result['powerjoular_power']} W")
                
            if 'CPU Utilization' in pj_df.columns:
                result['powerjoular_util'] = round(pj_df['CPU Utilization'].mean() * 100, 3)
                output.console_log(f"PowerJoular average CPU utilization: {result['powerjoular_util']}%")
                
        except Exception as e:
            output.console_log_FAIL(f"Error processing PowerJoular data: {str(e)}")
            
        return result
        
    def _find_primary_powerjoular_file(self, powerjoular_files: List[Path]) -> Path:
        base_powerjoular_file = None
        
        for file in powerjoular_files:
            if "-" not in file.name:
                base_powerjoular_file = file
                break
        
        if not base_powerjoular_file:
            base_powerjoular_file = max(powerjoular_files, key=lambda p: p.stat().st_mtime)
            output.console_log_FAIL(f"Could not find main PowerJoular file, using: {base_powerjoular_file}")
        else:
            output.console_log_OK(f"Found primary PowerJoular file: {base_powerjoular_file}")
            
        return base_powerjoular_file

    def _process_scenario_data(self, context: RunnerContext) -> Dict[str, Any]:
        result = {
            'scenario_count': 0,
            'test_success_rate': 0,
            'test_duration': 0
        }
        
        scenario_csv = context.run_dir / "scenario_summary.csv"
        if not scenario_csv.exists():
            output.console_log_FAIL("No scenario summary file found")
            return result
            
        try:
            scenario_data = self._read_scenario_csv(scenario_csv)
            
            succeeded_count = sum(1 for scenario in scenario_data if scenario.get('status') == 'Succeeded')
            total_count = len(scenario_data)
            
            if total_count == 0:
                return result
                
            result['scenario_count'] = total_count
            result['test_success_rate'] = round(succeeded_count / total_count * 100, 1)
            
            if scenario_data:
                result['test_duration'] = self._calculate_test_duration(scenario_data)
                
        except Exception as e:
            output.console_log_FAIL(f"Error processing scenario summary: {str(e)}")
            
        return result
        
    def _read_scenario_csv(self, csv_path: Path) -> List[Dict]:
        scenario_data = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                scenario_data.append(row)
        return scenario_data
        
    def _calculate_test_duration(self, scenario_data: List[Dict]) -> float:
        try:
            import datetime
            start_times = [datetime.datetime.strptime(s.get('startTime', ''), '%Y-%m-%d %H:%M:%S') 
                         for s in scenario_data if s.get('startTime')]
            end_times = [datetime.datetime.strptime(s.get('endTime', ''), '%Y-%m-%d %H:%M:%S') 
                       for s in scenario_data if s.get('endTime')]
            
            if start_times and end_times:
                min_start = min(start_times)
                max_end = max(end_times)
                duration_seconds = (max_end - min_start).total_seconds()
                return round(duration_seconds, 1)
            return 0
        except Exception as e:
            output.console_log_FAIL(f"Error calculating test duration: {str(e)}")
            return 0

    def _process_vm_metrics(self, context: RunnerContext) -> Dict[str, float]:
        """Extract only the VM metrics from power_log.csv, not the energy calculations"""
        result = {}
        power_log_file = context.run_dir / "power_log.csv"
        
        if not power_log_file.exists():
            output.console_log_FAIL("No power log file found in run directory")
            return {
                'vm_avg_cpu_percent': 0,
                'vm_avg_memory_mb': 0,
                'vm_avg_thread_count': 0,
                'vm_total_io_read_count': 0,
                'vm_total_io_write_count': 0
            }
            
        output.console_log(f"Found power log file: {power_log_file}")
        
        try:
            df = pd.read_csv(power_log_file)
            
            if 'cpu_percent' in df.columns:
                result['vm_avg_cpu_percent'] = round(df['cpu_percent'].mean(), 2)
                
            if 'memory_rss_mb' in df.columns:
                result['vm_avg_memory_mb'] = round(df['memory_rss_mb'].mean(), 2)
                
            if 'thread_count' in df.columns:
                result['vm_avg_thread_count'] = round(df['thread_count'].mean(), 2)
                
            if 'io_read_count' in df.columns and len(df) > 1:
                result['vm_total_io_read_count'] = int(df['io_read_count'].iloc[-1]) - int(df['io_read_count'].iloc[0])
            
            if 'io_write_count' in df.columns and len(df) > 1:
                result['vm_total_io_write_count'] = int(df['io_write_count'].iloc[-1]) - int(df['io_write_count'].iloc[0])
                
        except Exception as e:
            output.console_log_FAIL(f"Error processing VM metrics from power log: {str(e)}")
            result = {
                'vm_avg_cpu_percent': 0,
                'vm_avg_memory_mb': 0,
                'vm_avg_thread_count': 0,
                'vm_total_io_read_count': 0,
                'vm_total_io_write_count': 0
            }
            
        return result

    def populate_run_data(self, context: RunnerContext) -> Optional[Dict[str, Any]]:
        output.console_log("Processing measurement data and test results...")
        
        powerjoular_metrics = self._process_powerjoular_data(context)
        
        vm_metrics = self._process_vm_metrics(context)
        
        scenario_metrics = self._process_scenario_data(context)
        
        run_data = {**powerjoular_metrics, **vm_metrics, **scenario_metrics}
        
        output.console_log_OK("Run data populated successfully")
        return run_data

    def after_experiment(self) -> None:
        output.console_log("Cleaning up after experiment...")
        
        self._stop_power_logging()
        self._perform_final_cleanup()
            
        output.console_log_OK("Experiment cleanup completed")
        
    def _stop_power_logging(self):
        if hasattr(self, 'power_logger') and self.power_logger:
            self.power_logger.stop()
            
    def _perform_final_cleanup(self):
        try:
            app_vm = ExternalMachineAPI(
                hostname=self.app_vm_host,
                username=self.vm_username,
                password=self.vm_password
            )
            
            app_vm.execute_remote_command(
                "powershell -Command \"Stop-Process -Name 'Afas.Cqrs.Webserver','dotnet' -Force -ErrorAction SilentlyContinue\""
            )
            
            test_vm = ExternalMachineAPI(
                hostname=self.test_vm_host,
                username=self.vm_username,
                password=self.vm_password
            )
            
            test_vm.execute_remote_command(
                "powershell -Command \"Stop-Process -Name 'node','playwright','chrome','msedge','firefox' -Force -ErrorAction SilentlyContinue\""
            )
            
            output.console_log_OK("Final process cleanup completed")
        except Exception as e:
            output.console_log_FAIL(f"Final cleanup error: {str(e)}")

    # ================================ DO NOT ALTER BELOW THIS LINE ================================
    experiment_path:            Path             = None
