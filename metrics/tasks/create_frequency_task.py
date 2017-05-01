from engine_utils.util.targets.hive_table_target import HiveTableTarget
from engine_utils.util.tasks.spark_task import SparkTask
from engine.read.tasks.process_raw_media_data_task import ProcessRawMediaTask
from engine.mpa.tasks.generate_aoa_projection_factors_task \
    import GenerateAllOutletAdjustmentProjectionFactorsTask
from metrics.tasks.projtables_task import projTablesTask
from friendly_mapping_task import FriendlyMappingTask
from metrics.helpers.pdf_to_table_excel import pdf_to_excel, pdf_to_table
from metrics.helpers.make_media import make_df_dec, get_dimensions_from_configs
from pyspark.sql.functions import lit
import luigi
from metrics.lib.hh_freq import freq_main


class CreateFrequencyTask(SparkTask):
    """
    Frequency metrics for consulting
    brand_dbase: brand level database. Leave blank if using subbrand as brand database
    output_path (optional): output path of excel file. Default: /mroi/Xiao
    """
    brand_dbase = luigi.parameter.Parameter(default='')
    output_path = luigi.parameter.Parameter(default='/mroi/Xiao')

    def __init__(self, *args, **kwargs):
        super(CreateFrequencyTask, self).__init__(*args, **kwargs)
        self.mtypes = self.base_modeling_media.get_media_subtypes_list_from_supertype('dig')
        self.task_list = [ProcessRawMediaTask(media_type=mt, run_id=self.run_id)
                          for mt in self.mtypes]
        self.map_task = FriendlyMappingTask(run_id=self.run_id, brand_dbase=self.brand_dbase)
        self.projfact_task = GenerateAllOutletAdjustmentProjectionFactorsTask(
            run_id=self.run_id, media_type='dig')
        self.projtables_task = projTablesTask(run_id=self.run_id)

    @property
    def table_name(self):
        return 'consulting_freq'

    def requires(self):
        return [self.projfact_task, self.map_task, self.projtables_task] + self.task_list

    def main(self, sc, hc):
        map_df = hc.table(self.map_task.output().table)
        media_list = [hc.table(t.output()[0].table).withColumn('etype', lit(m))
                      for m, t in zip(self.mtypes, self.task_list)]
        proj_df = hc.table(self.projfact_task.output().table)
        # projtable_df = hc.table(self.projtables_task.output().table)
        # make_media = make_df_dec(proj_df, projtable_df)
        make_media = make_df_dec(proj_df)
        df_hier = get_dimensions_from_configs(hc, self.mta_config, self.base_modeling_media)
        freq_pdf = freq_main(make_media, media_list, map_df, self.mtypes, df_hier)
        pdf_to_table(hc, self.database_name, [self.table_name], [freq_pdf])
        pdf_to_excel(self.output_path, [self.table_name], [freq_pdf], self.database_name)

    def output(self):
        return HiveTableTarget(self.table_name, self.database_name)
