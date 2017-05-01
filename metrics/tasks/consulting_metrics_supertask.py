from engine_utils.util.targets.hive_table_target import HiveTableTarget
from engine_utils.util.tasks.spark_task import SparkTask
import luigi
from create_hh_metrics_task import CreateHHMetricsTask
from create_pairwise_task import CreatePairwiseTask
from create_reach_task import CreateReachTask
from create_reach_week_task import CreateReachWeekTask
from create_frequency_task import CreateFrequencyTask


class ConsultingSuperTask(SparkTask):
    """
    SuperTask wrapper for all sub consulting tasks
    Outputs all custom metrics for PowerPoint
    pair_dims (optional): dimensions to create pairwise #'s. Default: 'etype'
    output_path (optional): folder to output csvs. Default: /mroi/Xiao
    """
    brand_dbase = luigi.parameter.Parameter(default='')
    output_path = luigi.parameter.Parameter(default='/mroi/Xiao')

    def __init__(self, *args, **kwargs):
        super(ConsultingSuperTask, self).__init__(*args, **kwargs)
        hhm_task = CreateHHMetricsTask(run_id=self.run_id, output_path=self.output_path)
        pw_task = CreatePairwiseTask(run_id=self.run_id, output_path=self.output_path)
        rc_task = CreateReachTask(run_id=self.run_id, output_path=self.output_path,
                                  brand_dbase=self.brand_dbase)
        rcw_task = CreateReachWeekTask(run_id=self.run_id, output_path=self.output_path)
        freq_task = CreateFrequencyTask(run_id=self.run_id, output_path=self.output_path,
                                        brand_dbase=self.brand_dbase)
        self.tasks = [hhm_task, pw_task, rc_task, rcw_task, freq_task]
        self.table_name = freq_task.table_name

    def requires(self):
        return self.tasks

    def main(self, sc, hc):
        pass

    def output(self):
        return HiveTableTarget(self.table_name, self.database_name)
