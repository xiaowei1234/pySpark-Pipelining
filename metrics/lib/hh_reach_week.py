import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import LongType


def make_weekly(df, project=True):
    if not project:
        df.withColumn('projfact', F.lit(1).astype(LongType()))
    weeks = [r[0] for r in df.select('week').distinct().orderBy('week').collect()]
    df_dd = (df.select('week', 'household_id', 'projfact')
             .dropDuplicates(subset=['week', 'household_id']).cache())
    counts = []
    for w in weeks:
        df_before = (df_dd.where(df_dd.week < w).withColumnRenamed('week', 'wk')
                     .select('household_id', 'wk'))
        df_week = (df_dd.where(df_dd.week == w).join(df_before, on='household_id', how='left_outer')
                   .where(F.col('wk').isNull()).groupBy().agg(F.sum('projfact').alias('count'))
                   )
        counts.append(round(df_week.collect()[0][0]))
    df_dd.unpersist()
    pdf = pd.DataFrame({'week': weeks, 'counts': counts})[['week', 'counts']]
    pdf['cumsum'] = pdf.counts.cumsum()
    return pdf.rename(columns=lambda v: v.title())
