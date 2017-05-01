from engine_utils.util.targets.hive_table_target import HiveTableTarget
from engine_utils.util.tasks.spark_task import SparkTask
from engine.read.tasks.process_raw_media_data_task import ProcessRawMediaTask
from engine.mpa.tasks.generate_aoa_projection_factors_task \
    import GenerateAllOutletAdjustmentProjectionFactorsTask
from samo.tasks.generate_keno_univ_tables import BuildKenoUnivTables
from metrics.helpers.make_media import get_dimensions_from_configs
from metrics.helpers.pdf_to_table_excel import pdf_to_table, pdf_to_excel
from metrics.lib.hh_project_univ import main as pmain
import luigi


class projTablesTask(SparkTask):
    """child task for projecting households to universe exposures"""
    brand_dbase = luigi.parameter.Parameter(default='')
    output_path = luigi.parameter.Parameter(default='/mroi/Xiao')

    def __init__(self, *args, **kwargs):
        super(projTablesTask, self).__init__(*args, **kwargs)
        self.mtypes = self.base_modeling_media.get_media_subtypes_list_from_supertype('dig')
        self.task_list = [ProcessRawMediaTask(media_type=mt, run_id=self.run_id)
                          for mt in self.mtypes]
        self.projfact_task = GenerateAllOutletAdjustmentProjectionFactorsTask(
            run_id=self.run_id, media_type='dig')
        self.univtable_tasks = [BuildKenoUnivTables(run_id=self.run_id, media_type=m)
                                for m in self.mtypes]

    @property
    def table_name(self):
        return 'consulting_project'

    def requires(self):
        return [self.projfact_task] + self.task_list + self.univtable_tasks

    def main(self, sc, hc):
        proj_df = hc.table(self.projfact_task.output().table)
        media_list = [hc.table(t.output()[0].table) for t in self.task_list]
        univ_list = [hc.table(t.output().table) for t in self.univtable_tasks]
        df_hier = get_dimensions_from_configs(hc, self.mta_config, self.base_modeling_media)
        pdf = pmain(media_list, proj_df, univ_list, self.mtypes, df_hier)
        pdf_to_excel(self.output_path, [self.table_name], [pdf], self.database_name)
        pdf_to_table(hc, self.database_name, [self.table_name], [pdf])

    def output(self):
        return HiveTableTarget(self.table_name, self.database_name)
