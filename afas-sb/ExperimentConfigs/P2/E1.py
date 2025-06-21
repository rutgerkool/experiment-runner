import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from RunnerConfig import *

class RunnerConfig(RunnerConfig):
    
    name = "P2_E1"
    
    def create_run_table_model(self) -> RunTableModel:
        parallelism_config_factor = FactorModel("parallelism_config", [
            "parallel_4_lookahead_default",
            "parallel_16_lookahead_default",
            "parallel_default_lookahead_12",
            "parallel_default_lookahead_48"
        ])
        
        self.run_table_model = RunTableModel(
            factors=[parallelism_config_factor],
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
        parallelism_config = context.run_variation["parallelism_config"]
        
        if parallelism_config == "parallel_4_lookahead_default":
            env_var_setting = "$env:DefaultMaxParallelism = 4"
            config_desc = "parallelism=4, lookahead=default"
        elif parallelism_config == "parallel_16_lookahead_default":
            env_var_setting = "$env:DefaultMaxParallelism = 16"
            config_desc = "parallelism=16, lookahead=default"
        elif parallelism_config == "parallel_default_lookahead_12":
            env_var_setting = "$env:DefaultMaxLookAhead = 12"
            config_desc = "parallelism=default, lookahead=12"
        elif parallelism_config == "parallel_default_lookahead_48":
            env_var_setting = "$env:DefaultMaxLookAhead = 48"
            config_desc = "parallelism=default, lookahead=48"
        else:
            raise ValueError(f"Unsupported parallelism configuration: {parallelism_config}")
        
        powershell_command = f"""powershell -Command "Set-Location -Path 'C:\\anta\\sb'; Remove-Item env:DefaultMaxParallelism -ErrorAction SilentlyContinue; Remove-Item env:DefaultMaxLookAhead -ErrorAction SilentlyContinue; {env_var_setting}; Write-Host 'Starting with {config_desc}'; .\\run.ps1" """
        
        output.console_log(f"Starting .NET application with {config_desc}...")

        app_log_file = context.run_dir / "app_vm_startup.log"
        config_file = context.run_dir / "appsettings.json"

        app_vm = ExternalMachineAPI(
            hostname=self.app_vm_host,
            username=self.vm_username,
            password=self.vm_password
        )
        
        backup_command = """powershell -Command "
            if (-not (Test-Path 'C:\\anta\\config_backups')) {
                New-Item -Path 'C:\\anta\\config_backups' -ItemType Directory -Force
            }
            if (-not (Test-Path 'C:\\anta\\config_backups\\appsettings.json.original')) {
                Copy-Item 'C:\\anta\\sb\\appsettings.json' 'C:\\anta\\config_backups\\appsettings.json.original'
            } else {
                Copy-Item 'C:\\anta\\config_backups\\appsettings.json.original' 'C:\\anta\\sb\\appsettings.json' -Force
            }
        """
        app_vm.execute_remote_command(command=backup_command)
        
        time.sleep(5)

        output.console_log("Capturing configuration settings...")
        app_vm.copy_file_from_remote("C:\\anta\\sb\\appsettings.json", str(config_file))
        
        success = app_vm.execute_remote_command(
            command=powershell_command,
            wait_for_output=self.app_ready_text,
            log_file=app_log_file
        )

        if not success:
            output.console_log_FAIL(f"Failed to start .NET application with {config_desc}")
        else:
            output.console_log_OK(f".NET application started successfully with {config_desc}")

        if app_log_file.exists():
            output.console_log(f"Application startup log saved to: {app_log_file}")
            output.console_log(f"Log file size: {app_log_file.stat().st_size} bytes")
        
        if config_file.exists():
            output.console_log(f"Configuration file saved to: {config_file}")
            output.console_log(f"Config file size: {config_file.stat().st_size} bytes")
