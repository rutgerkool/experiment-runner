import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from RunnerConfig import *

class RunnerConfig(RunnerConfig):
    
    name = "P3_E1"
    
    def create_run_table_model(self) -> RunTableModel:
        optimization_factor = FactorModel("optimization_config", [
            "baseline",
            "carbon_optimized"
        ])
        
        self.run_table_model = RunTableModel(
            factors=[optimization_factor],
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
        optimization_config = context.run_variation["optimization_config"]
        
        output.console_log(f"Starting .NET application with optimization configuration: {optimization_config}...")

        app_log_file = context.run_dir / "app_vm_startup.log"
        config_file = context.run_dir / "appsettings.json"

        app_vm = ExternalMachineAPI(
            hostname=self.app_vm_host,
            username=self.vm_username,
            password=self.vm_password
        )
        
        if optimization_config == "baseline":
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
            
            run_command = "powershell -Command \"Set-Location -Path 'C:\\anta\\sb'; Remove-Item env:DefaultMaxParallelism -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_gcServer -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_gcConcurrent -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCDynamicAdaptationMode -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCRetainVM -ErrorAction SilentlyContinue; Write-Host 'Baseline configuration'; .\\run.ps1\""
            
        elif optimization_config == "carbon_optimized":
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
            
            time.sleep(2)
            
            config_command = 'powershell -File "C:\\anta\\set_p3_optimized_config.ps1"'
            app_vm.execute_remote_command(command=config_command)
            
            run_command = """powershell -Command "Set-Location -Path 'C:\\anta\\sb'; Remove-Item env:DefaultMaxParallelism -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_gcServer -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_gcConcurrent -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCDynamicAdaptationMode -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCRetainVM -ErrorAction SilentlyContinue; $env:DefaultMaxParallelism = 16; $env:DOTNET_gcServer = '1'; $env:DOTNET_gcConcurrent = '1'; $env:DOTNET_GCDynamicAdaptationMode = '1'; $env:DOTNET_GCRetainVM = '0'; Write-Host 'Carbon-optimized: 16 threads + DynamicAdaptation GC + minimal logging + extended cache'; .\\run.ps1" """
            
        else:
            raise ValueError(f"Unsupported optimization configuration: {optimization_config}")
        
        time.sleep(5)

        output.console_log("Capturing applied configuration file...")
        app_vm.copy_file_from_remote("C:\\anta\\sb\\appsettings.json", str(config_file))
        
        success = app_vm.execute_remote_command(
            command=run_command,
            wait_for_output=self.app_ready_text,
            log_file=app_log_file
        )

        if not success:
            output.console_log_FAIL(f"Failed to start .NET application with optimization configuration: {optimization_config}")
        else:
            output.console_log_OK(f".NET application started successfully with optimization configuration: {optimization_config}")

        if app_log_file.exists():
            output.console_log(f"Application startup log saved to: {app_log_file}")
            output.console_log(f"Log file size: {app_log_file.stat().st_size} bytes")
        
        if config_file.exists():
            output.console_log(f"Configuration file saved to: {config_file}")
            output.console_log(f"Config file size: {config_file.stat().st_size} bytes")
