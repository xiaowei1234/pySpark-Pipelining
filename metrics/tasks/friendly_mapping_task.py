from engine_utils.util.tasks.spark_task import SparkTask
from engine_utils.util.targets.hive_table_target import HiveTableTarget
from engine.read.tasks.process_raw_media_data_task import ProcessRawMediaTask
from engine_utils.util.tasks.external_table_task import ExternalTableTask
from metrics.helpers.make_media import union_df_list
from metrics.lib.hh_mapping import make_map
from pyspark.sql.functions import lit
import luigi


class FriendlyMappingTask(SparkTask):
    """
    part of ReachFrequencyTask
    """
    brand_dbase = luigi.parameter.Parameter(default='')

    @property
    def table_name(self):
        return 'consulting_name_map'

    def __init__(self, *args, **kwargs):
        super(FriendlyMappingTask, self).__init__(*args, **kwargs)
        self.mtypes = self.base_modeling_media.get_media_subtypes_list_from_supertype('dig')
        # self.mtypes = [m for m in self.mtypes if m in ['dig', 'oao']]
        self.task_list = [ProcessRawMediaTask(media_type=mt, run_id=self.run_id)
                          for mt in self.mtypes]
        if len(self.brand_dbase) == 0:
            self.brand_dbase = self.database_name
        self.samo_adserver_task = ExternalTableTask(table='samo_keno_meta_adserver',
                                                    database=self.brand_dbase)

    def requires(self):
        return [self.samo_adserver_task] + self.task_list

    def main(self, sc, hc):
        adserver_df = hc.table(self.brand_dbase + '.' + self.samo_adserver_task.output().table)
        df_list = [hc.table(t.output()[1].table).withColumn('etype', lit(m.lower()))
                   for t, m in zip(self.task_list, self.mtypes)]
        map_df = union_df_list(df_list)
        df = make_map(adserver_df, map_df)
        df.write.saveAsTable(self.database_name + '.' + self.table_name)

    def output(self):
        return HiveTableTarget(self.table_name, self.database_name)
