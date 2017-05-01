import pyspark.sql.functions as F
from pyspark.sql.types import LongType, ArrayType, StringType
from metrics.helpers.name_mapping import dimension_to_friendlyname


def overall_numbers(df, dim):
    pdf = (df.drop_duplicates(['household_id', dim]).groupBy(dim)
           .agg(F.sum('projfact').alias('cnt'))
           .toPandas()
           )
    return pdf.set_index(dim)


def pair_overlap(df, d, myfunc):
    myudf = F.udf(myfunc, ArrayType(elementType=ArrayType(StringType())))
    df_paired = (df.groupBy('household_id', 'projfact')
                 .agg(F.collect_set(d).alias('aset'))
                 .where(F.size('aset') > 1)
                 .withColumn('pairs', myudf('aset'))
                 .withColumn('apair', F.explode(F.col('pairs')))
                 .select('household_id', 'projfact',
                         F.col('apair').getItem(0).alias('Media Type 1'),
                         F.col('apair').getItem(1).alias('Media Type 2')
                         )
                 .groupBy('Media Type 1', 'Media Type 2')
                 .agg(F.sum('projfact').alias('Overlapping Households'))
                 )
    return df_paired.toPandas()


def make_final_pair(pdf_overlap, pdf_overall):
    pdf_overlap["Pct of Media Type 1"] = pdf_overlap['Overlapping Households'] / \
        pdf_overlap['Media Type 1'].apply(lambda v: pdf_overall.loc[v, 'cnt'])
    pdf_overlap["Pct of Media Type 2"] = pdf_overlap['Overlapping Households'] / \
        pdf_overlap['Media Type 2'].apply(lambda v: pdf_overall.loc[v, 'cnt'])
    return pdf_overlap


def redo_columns(pdf, df_hier):
    cs = ['Media Type 1', 'Media Type 2']
    pdf[cs] = dimension_to_friendlyname(pdf[cs], df_hier.toPandas())
    pdf_sorted = pdf.sort_values('Overlapping Households', ascending=False)
    pdf_sorted['Overlapping Households'] = pdf_sorted[
        'Overlapping Households'].apply(lambda v: round(v))
    return pdf_sorted[['Media Type 1', 'Media Type 2', 'Overlapping Households',
                       'Pct of Media Type 1', 'Pct of Media Type 2'
                       ]]


def make_pairwise(make_media, media_list, dims, df_hier, myfunc, project=True):
    pairwise = []
    for d in dims:
        df = make_media(media_list, d)
        if not project:
            df = df.withColumn('projfact', F.lit(1).astype(LongType()))
        pdf_overlap = pair_overlap(df, d, myfunc)
        pdf_overall = overall_numbers(df, d)
        pdf_pair = make_final_pair(pdf_overlap, pdf_overall)
        pdf_renamed = redo_columns(pdf_pair, df_hier)
        pairwise.append(pdf_renamed)
    return pairwise
