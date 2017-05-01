from engine_utils.util.targets.hive_table_target import HiveTableTarget
from engine_utils.util.tasks.spark_task import SparkTask
from pyspark import SparkConf
import luigi
from engine.read.tasks.process_raw_media_data_task import ProcessRawMediaTask
from engine.mpa.tasks.create_household_universe_task import CreateHouseholdUniverseTask
from engine_utils.util.tasks.external_table_task import ExternalTableTask
from metrics.helpers.pdf_to_table_excel import pdf_to_excel, pdf_to_table
from metrics.lib.hh_before_after import main as metrics_main


class CreateHHMetricsTask(SparkTask):
    """
    creates unit share metrics as well as contingency table
    unit share week min= 1 and max = 13
    contingency table is 6 weeks

    purchase_table (optional): database.tablename of historical data.
        If blank assumes model database table: purch_aggregate_trip_purchase_detail
    output_path (optional): output path of csv files. default '/mroi/Xiao'
    """
    purchase_table = luigi.parameter.Parameter(default=None)
    output_path = luigi.parameter.Parameter(default='/mroi/Xiao')

    @property
    def table_name(self):
        return 'consulting_share_metrics'

    @property
    def table_cont(self):
        return 'consulting_contingency_table'

    @property
    def spark_conf(self):
        conf = SparkConf()
        conf.setMaster("yarn-client")
        conf.set("spark.executor.instances", "7")
        conf.set("spark.executor.memory", "20g")
        conf.set("spark.executor.cores", "16")
        conf.set("spark.yarn.executor.memoryOverhead", "4000")
        conf.set("spark.sql.shuffle.partitions", "500")
        conf.set("spark.driver.maxResultSize", "0")
        return conf

    def __init__(self, *args, **kwargs):
        super(CreateHHMetricsTask, self).__init__(*args, **kwargs)
        mtypes = self.base_modeling_media.get_media_subtypes_list_from_supertype('dig')
        self.media_tasks = []
        for med_sub in mtypes:
            task = ProcessRawMediaTask(media_type=med_sub, run_id=self.run_id)
            self.media_tasks.append(task)
        self.universe_task = CreateHouseholdUniverseTask(run_id=self.run_id, media_type='dig')
        if self.purchase_table is None:
            self.purch_task = ExternalTableTask(table='purch_aggregate_trip_purchase_detail',
                                                database=self.database_name)
        else:
            tab = self.purchase_table.split('.')
            self.purch_task = ExternalTableTask(table=tab[1], database=tab[0])

    def requires(self):
        return [self.universe_task, self.purch_task] + self.media_tasks

    def main(self, sc, hc):
        df_purch = hc.table(self.purch_task.output().table).withColumnRenamed('date', 'pdate')
        df_univ = hc.table(self.universe_task.output().table)
        df_list = [hc.table(t.output()[0].table) for t in self.media_tasks]
        share_pdf, cont_pdf = metrics_main(df_purch, df_list, df_univ)
        tables = [self.table_name, self.table_cont]
        pdfs = [share_pdf, cont_pdf]
        pdf_to_table(hc, self.database_name, tables, pdfs)
        pdf_to_excel(self.output_path, tables, pdfs, self.database_name)

    def output(self):
        return [
            HiveTableTarget(self.table_name, self.database_name),
            HiveTableTarget(self.table_cont, self.database_name)
        ]
