from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from metrics.helpers.name_mapping \
    import create_friendly_df, none_to_empty, dimension_to_friendlyname
from metrics.helpers.make_media import get_hierarchy_from_df


def freq_dec(func):
    def calc_freq(*args):
        df, dim = func(*args)
        pdf_freqs = (df.withColumn('Frequency',
                     F.when(df.frequency >= 10, 10)
                        .otherwise(df.frequency))
                     .groupBy('Frequency').agg(F.sum('projfact').alias('Proj HH'))
                     .toPandas()
                     )
        pdf_freqs['Dimension'] = dim
        pdf_freqs['Pct HH'] = pdf_freqs['Proj HH'] / pdf_freqs['Proj HH'].sum()
        pdf_freqs['Proj HH'] = pdf_freqs['Proj HH'].apply(lambda v: round(v)).astype(int)
        return pdf_freqs
    return calc_freq


@freq_dec
def expo_freq(df):
    df_cnt = (df.groupBy('household_id')
              .agg(F.count('projfact').alias('frequency'),
                   F.max('projfact').alias('projfact'))
              )
    return df_cnt, 'Exposures'


@freq_dec
def get_one_freq(df, dim):
    df_cnt = (df.drop_duplicates(['household_id', dim])
              .groupBy('household_id').agg(F.count('projfact').alias('frequency'),
                                           F.max('projfact').alias('projfact'))
              )
    return df_cnt, dim


def make_overall(make_media, media_list):
    pdf = expo_freq(make_media(media_list, 'date'))
    pdf['Type'] = 'All'
    pdf_etype = get_one_freq(make_media(media_list, 'etype'), 'etype')
    pdf_etype['Dimension'] = 'Event Type'
    pdf_etype['Type'] = 'All'
    return pdf.append(pdf_etype)


def freq_main(make_media, media_list, map_df, mtypes, df_hier):
    freq_pdf = make_overall(make_media, media_list)
    for m, df in zip(mtypes, media_list):
        dims = none_to_empty(get_hierarchy_from_df(df_hier, m))
        for d in dims:
            adf, name_pdf = create_friendly_df(make_media, df, map_df, d)
            dim_pdf = get_one_freq(adf, 'val_id')
            dim_pdf['Dimension'] = d
            dim_pdf['Type'] = m
            freq_pdf = freq_pdf.append(dim_pdf)
    freq_pdf[['Type', 'Dimension']] = dimension_to_friendlyname(
        freq_pdf[['Type', 'Dimension']], df_hier.toPandas())
    return freq_pdf[['Type', 'Dimension', 'Frequency', 'Proj HH', 'Pct HH']]
