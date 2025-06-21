import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from RunnerConfig import *

class RunnerConfig(RunnerConfig):

    name = "P2_E4"
    
    def create_run_table_model(self) -> RunTableModel:
        compression_factor = FactorModel("compression_config", ["no_compression"])
        
        self.run_table_model = RunTableModel(
            factors=[compression_factor],
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
    
    def start_run(self, context: RunnerContext) -> None:
        compression_mode = context.run_variation["compression_config"]
        enable_response_compression = "true" if compression_mode != "no_compression" else "false"
        
        output.console_log(f"Starting .NET application on VM with compression = {compression_mode}...")

        app_log_file = context.run_dir / "app_vm_startup.log"
        config_file = context.run_dir / "appsettings.json"

        app_vm = ExternalMachineAPI(
            hostname=self.app_vm_host,
            username=self.vm_username,
            password=self.vm_password
        )
        
        update_command = f'powershell -File "C:\\anta\\set_compression_config.ps1" -enableResponseCompression "{enable_response_compression}"'
        app_vm.execute_remote_command(command=update_command)
        
        time.sleep(5)
        
        output.console_log("Capturing applied configuration file...")
        app_vm.copy_file_from_remote("C:\\anta\\sb\\appsettings.json", str(config_file))
        
        run_command = "powershell -Command \"Set-Location -Path 'C:\\anta\\sb'; .\\run.ps1\""
        success = app_vm.execute_remote_command(
            command=run_command,
            wait_for_output=self.app_ready_text,
            log_file=app_log_file
        )

        if not success:
            output.console_log_FAIL(f"Failed to start .NET application with compression mode: {compression_mode}")
        else:
            output.console_log_OK(f".NET application started successfully with compression mode: {compression_mode}")

        if app_log_file.exists():
            output.console_log(f"Application startup log saved to: {app_log_file}")
            output.console_log(f"Log file size: {app_log_file.stat().st_size} bytes")
        
        if config_file.exists():
            output.console_log(f"Configuration file saved to: {config_file}")
            output.console_log(f"Config file size: {config_file.stat().st_size} bytes")
