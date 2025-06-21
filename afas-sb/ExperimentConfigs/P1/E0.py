import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from RunnerConfig import *

class RunnerConfig(RunnerConfig):
    
    name = "P1_E0"

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
            repetitions = 5
        )
        return self.run_table_model
