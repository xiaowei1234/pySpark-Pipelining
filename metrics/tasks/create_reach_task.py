from engine_utils.util.targets.hive_table_target import HiveTableTarget
from engine_utils.util.tasks.spark_task import SparkTask
from engine.read.tasks.process_raw_media_data_task import ProcessRawMediaTask
# from engine.read.tasks.process_purchase_data_task import ProcessPurchaseDataTask
from engine.mpa.tasks.generate_aoa_projection_factors_task \
    import GenerateAllOutletAdjustmentProjectionFactorsTask
from friendly_mapping_task import FriendlyMappingTask
from pyspark.sql.functions import lit
from metrics.tasks.projtables_task import projTablesTask
from metrics.helpers.pdf_to_table_excel import pdf_to_excel, pdf_to_table
from metrics.helpers.make_media import make_df_dec, get_dimensions_from_configs
from metrics.lib.hh_reach import main as hhmain
import luigi


class CreateReachTask(SparkTask):
    """
    Outputs custom reach metrics for PowerPoint
    pair_dims (optional): dimensions to create pairwise #'s. Default: 'etype'
    brand_dbase (optional): if not running in database with samo tables
    output_path (optional): folder to output csvs. Default: /mroi/Xiao
    """
    brand_dbase = luigi.parameter.Parameter(default='')
    output_path = luigi.parameter.Parameter(default='/mroi/Xiao')

    def __init__(self, *args, **kwargs):
        super(CreateReachTask, self).__init__(*args, **kwargs)
        self.mtypes = self.base_modeling_media.get_media_subtypes_list_from_supertype('dig')
        # self.mtypes = [m for m in self.mtypes if m in ['dig', 'oao']]
        self.task_list = [ProcessRawMediaTask(media_type=mt, run_id=self.run_id)
                          for mt in self.mtypes]
        self.map_task = FriendlyMappingTask(run_id=self.run_id, brand_dbase=self.brand_dbase)
        self.projfact_task = GenerateAllOutletAdjustmentProjectionFactorsTask(
            run_id=self.run_id, media_type='dig')
        self.projtables_task = projTablesTask(run_id=self.run_id)

    @property
    def table_name(self):
        return 'consulting_reach'

    def requires(self):
        return [self.projfact_task, self.map_task, self.projtables_task] + self.task_list

    def main(self, sc, hc):
        media_list = [hc.table(t.output()[0].table).withColumn('etype', lit(m))
                      for m, t in zip(self.mtypes, self.task_list)]
        proj_df = hc.table(self.projfact_task.output().table)
        # projtable_df = hc.table(self.projtables_task.output().table)
        # make_media = make_df_dec(proj_df, projtable_df)
        make_media = make_df_dec(proj_df)
        map_df = hc.table(self.map_task.output().table)
        df_hier = get_dimensions_from_configs(hc, self.mta_config, self.base_modeling_media)
        # call main code
        reach_pdf = hhmain(make_media, media_list, self.mtypes, map_df, df_hier)
        pdf_to_table(hc, self.database_name, [self.table_name], [reach_pdf])
        pdf_to_excel(self.output_path, [self.table_name], [reach_pdf], self.database_name)

    def output(self):
        return HiveTableTarget(self.table_name, self.database_name)
