import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from RunnerConfig import *

class RunnerConfig(RunnerConfig):
    name = "P2_E5"
    
    def create_run_table_model(self) -> RunTableModel:
        gc_config_factor = FactorModel("gc_config", [
            "MemoryConservation",
            "DynamicAdaptation",
            "ThreadEfficient",
            "ReducedFootprint"
        ])
        
        self.run_table_model = RunTableModel(
            factors=[gc_config_factor],
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
        gc_config = context.run_variation["gc_config"]
        
        if gc_config == "MemoryConservation":
            gc_env_vars = "$env:DOTNET_gcServer = '1'; $env:DOTNET_gcConcurrent = '1'; $env:DOTNET_GCConserveMemory = '7'; $env:DOTNET_GCLOHThreshold = '140000'"
        elif gc_config == "DynamicAdaptation":
            gc_env_vars = "$env:DOTNET_gcServer = '1'; $env:DOTNET_gcConcurrent = '1'; $env:DOTNET_GCDynamicAdaptationMode = '1'; $env:DOTNET_GCRetainVM = '0'"
        elif gc_config == "ThreadEfficient":
            gc_env_vars = "$env:DOTNET_gcServer = '1'; $env:DOTNET_gcConcurrent = '1'; $env:DOTNET_GCNoAffinitize = '1'; $env:DOTNET_GCHeapCount = '4'"
        elif gc_config == "ReducedFootprint":
            gc_env_vars = "$env:DOTNET_gcServer = '1'; $env:DOTNET_gcConcurrent = '1'; $env:DOTNET_GCRetainVM = '0'; $env:DOTNET_GCHighMemPercent = '46'; $env:DOTNET_GCConserveMemory = '5'"
        else:
            raise ValueError(f"Unsupported GC configuration: {gc_config}")
        
        powershell_command = f"""powershell -Command "Set-Location -Path 'C:\\anta\\sb'; Remove-Item env:DOTNET_gcServer -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_gcConcurrent -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCConserveMemory -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCLOHThreshold -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCDynamicAdaptationMode -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCRetainVM -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCNoAffinitize -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCHeapCount -ErrorAction SilentlyContinue; Remove-Item env:DOTNET_GCHighMemPercent -ErrorAction SilentlyContinue; {gc_env_vars}; Write-Host 'Starting with {gc_config} GC config'; .\\run.ps1" """
        
        output.console_log(f"Starting .NET application on VM with GC config: {gc_config}...")
        
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
            output.console_log_FAIL(f"Failed to start .NET application with {gc_config} GC configuration")
        else:
            output.console_log_OK(f".NET application started successfully with {gc_config} GC configuration")
        
        if app_log_file.exists():
            output.console_log(f"Application startup log saved to: {app_log_file}")
            output.console_log(f"Log file size: {app_log_file.stat().st_size} bytes")
        
        if config_file.exists():
            output.console_log(f"Configuration file saved to: {config_file}")
            output.console_log(f"Config file size: {config_file.stat().st_size} bytes")
