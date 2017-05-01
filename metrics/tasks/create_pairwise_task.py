from engine_utils.util.targets.hive_table_target import HiveTableTarget
from engine_utils.util.tasks.spark_task import SparkTask
from engine.read.tasks.process_raw_media_data_task import ProcessRawMediaTask
from engine.mpa.tasks.generate_aoa_projection_factors_task \
    import GenerateAllOutletAdjustmentProjectionFactorsTask
from pyspark.sql.functions import lit
from metrics.tasks.projtables_task import projTablesTask
from metrics.helpers.make_media import make_df_dec, get_dimensions_from_configs
from metrics.helpers.pdf_to_table_excel import pdf_to_excel, pdf_to_table
from metrics.helpers.name_mapping import input_to_list
import luigi
from metrics.lib.hh_pairwise2 import make_pairwise
import os

with open('dummyModule.py', 'w') as fh:
    fh.write(
"""
def set_to_lists(alist):
    if len(alist) <= 2:
        return [sorted(alist)]
    element = alist[0]
    return [sorted([element, e]) for e in alist[1:]] + set_to_lists(alist[1:])
"""
    )


class CreatePairwiseTask(SparkTask):
    """dimensions: dimensions in metrics to create pairwise tables (default: etype)
    """
    dimensions = luigi.parameter.Parameter(default='etype')
    output_path = luigi.parameter.Parameter(default='/mroi/Xiao')

    def __init__(self, *args, **kwargs):
        super(CreatePairwiseTask, self).__init__(*args, **kwargs)
        self.dims = input_to_list(self.dimensions)
        self.projfact_task = GenerateAllOutletAdjustmentProjectionFactorsTask(
            run_id=self.run_id, media_type='dig')
        self.mtypes = self.base_modeling_media.get_media_subtypes_list_from_supertype('dig')
        # self.mtypes = [m for m in self.mtypes if m in ['dig', 'oao']]
        self.task_list = [ProcessRawMediaTask(media_type=mt, run_id=self.run_id)
                          for mt in self.mtypes]
        self.projtables_task = projTablesTask(run_id=self.run_id)

    def requires(self):
        return [self.projfact_task, self.projtables_task] + self.task_list

    @property
    def table_name(self):
        return ['consulting_pairwise_' + p for p in self.dims]

    def main(self, sc, hc):
        sc.addPyFile("dummyModule.py")
        media_list = [hc.table(t.output()[0].table).withColumn('etype', lit(m))
                      for m, t in zip(self.mtypes, self.task_list)]
        proj_df = hc.table(self.projfact_task.output().table)
        # projtable_df = hc.table(self.projtables_task.output().table)
        # make_media = make_df_dec(proj_df, projtable_df)
        make_media = make_df_dec(proj_df)
        df_hier = get_dimensions_from_configs(hc, self.mta_config, self.base_modeling_media)
        from dummyModule import set_to_lists
        pair_list = make_pairwise(make_media, media_list, self.dims, df_hier, set_to_lists)
        os.remove("dummyModule.py")
        pdf_to_table(hc, self.database_name, self.table_name, pair_list)
        pdf_to_excel(self.output_path, self.table_name, pair_list, self.database_name)

    def output(self):
        return [HiveTableTarget(p, self.database_name) for p in self.table_name]
