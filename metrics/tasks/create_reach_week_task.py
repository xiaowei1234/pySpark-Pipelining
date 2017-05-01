from engine_utils.util.targets.hive_table_target import HiveTableTarget
from engine_utils.util.tasks.spark_task import SparkTask
from engine.read.tasks.process_raw_media_data_task import ProcessRawMediaTask
from engine.mpa.tasks.generate_aoa_projection_factors_task \
    import GenerateAllOutletAdjustmentProjectionFactorsTask
from pyspark.sql.functions import lit
from metrics.tasks.projtables_task import projTablesTask
from metrics.helpers.make_media import make_df_dec
from metrics.helpers.pdf_to_table_excel import pdf_to_excel, pdf_to_table
from metrics.lib.hh_reach_week import make_weekly
import luigi


class CreateReachWeekTask(SparkTask):
    """output_path (optional): output directory of excel file (default /mroi/xiao)
    """
    output_path = luigi.parameter.Parameter(default='/mroi/Xiao')

    def __init__(self, *args, **kwargs):
        super(CreateReachWeekTask, self).__init__(*args, **kwargs)
        self.mtypes = self.base_modeling_media.get_media_subtypes_list_from_supertype('dig')
        self.task_list = [ProcessRawMediaTask(media_type=mt, run_id=self.run_id)
                          for mt in self.mtypes]
        self.projfact_task = GenerateAllOutletAdjustmentProjectionFactorsTask(
            run_id=self.run_id, media_type='dig')
        self.projtables_task = projTablesTask(run_id=self.run_id)

    @property
    def table_name(self):
        return 'consulting_week_reach'

    def requires(self):
        return [self.projfact_task, self.projtables_task] + self.task_list

    def main(self, sc, hc):
        media_list = [hc.table(t.output()[0].table).withColumn('etype', lit(m))
                      for m, t in zip(self.mtypes, self.task_list)]
        proj_df = hc.table(self.projfact_task.output().table)
        projtable_df = hc.table(self.projtables_task.output().table)
        # make_media = make_df_dec(proj_df, projtable_df)
        make_media = make_df_dec(proj_df)
        week_pdf = make_weekly(make_media(media_list, 'week'))
        pdf_to_table(hc, self.database_name, [self.table_name], [week_pdf])
        pdf_to_excel(self.output_path, [self.table_name], [week_pdf], self.database_name)

    def output(self):
        return HiveTableTarget(self.table_name, self.database_name)
