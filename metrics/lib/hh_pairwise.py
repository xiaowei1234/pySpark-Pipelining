import pyspark.sql.functions as F
from pyspark.sql.types import LongType
import numpy as np
import pandas as pd
from metrics.helpers.name_mapping import dimension_to_friendlyname


def redo_row(row):
    names = row[row == 1].index.values
    to_add = np.repeat('', len(row) - len(names))
    array = np.append(names, to_add)
    new_index = ['Media Type {}'.format(i) for i in xrange(1, len(row) + 1)]
    return pd.Series(array, new_index)


def redo_columns(pdf, df_hier):
    cs = [c for c in pdf.columns if c != 'count']
    pdf_sorted = pdf.sort_values(cs, ascending=False)
    new_pdf = pdf_sorted.apply(redo_row, 1)
    new_pdf2 = dimension_to_friendlyname(new_pdf, df_hier.toPandas())
    new_pdf2['Overlapping Projected Number of Households'] = pdf_sorted['count']
    return new_pdf2


def make_pairwise(make_media, media_list, dims, df_hier, project=True):
    pairwise = []
    for d in dims:
        df = make_media(media_list, d)
        if not project:
            df = df.withColumn('projfact', F.lit(1).astype(LongType()))
        df_p = (df.withColumn('count', F.lit(1).astype(LongType()))
                .groupBy('household_id', 'projfact').pivot(d).max('count')
                )
        agg_cols = [c for c in df_p.columns if c not in ('household_id', 'projfact')]
        df_pair = df_p.fillna(0).groupBy(agg_cols).agg(F.sum('projfact').alias('count'))
        cols = df_pair.columns
        cols.remove('count')
        cols.sort()
        pdf = df_pair.toPandas()[cols + ['count']]
        renamed_pdf = redo_columns(pdf, df_hier)
        pairwise.append(renamed_pdf)
    return pairwise
