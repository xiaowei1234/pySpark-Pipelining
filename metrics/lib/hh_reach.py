import pyspark.sql.functions as F
from metrics.helpers.name_mapping \
    import create_friendly_df, none_to_empty, dimension_to_friendlyname
from metrics.helpers.make_media import get_hierarchy_from_df


def add_uniq_col(df, cnt_col, dim):
    df_dd = df.dropna(subset=[dim]).dropDuplicates([cnt_col, dim])
    df_grouped = df_dd.groupby(cnt_col).count()
    return (df_dd.join(df_grouped, on=cnt_col)
            .withColumn('unique', F.when(F.col('count') > 1, 0).otherwise(1))
            .drop('count')
            )


def reach_count(df, cnt_col, dim=None, project=None):
    """
    returns pandas dataframe with counts grouped by optional additional dimensions
    df (param): spark dataframe that contains cnt_col, dim, and project column if specified
    cnt_col (param): (string or dataframe column): column to get counts by
    dim (param): optional (list of strings or dataframe columns): dimensions to get count by
    project (param): optional (string or dataframe column): projection factor
    note that cnt_col and elements of dim have to be all strings or all dataframe columns
    """
    if project is None:
        project = 'projfact'
        df = df.withColumn(project, F.lit(1))
    if dim is None:
        dim = 'all'
        df2 = df.withColumn('all', F.lit(''))
    else:
        df2 = df.dropna(subset=[dim])
    df_uniq = add_uniq_col(df2, cnt_col, dim)
    df_grouped = df_uniq.withColumn('unique', F.col('unique') * F.col(project)).groupBy(dim)
    df_counts = (df_grouped.agg(F.round(F.sum(project)).alias('count'),
                                F.round(F.sum('unique')).alias('unique'))
                 .withColumn('duplicates', F.col('count') - F.col('unique'))
                 )
    df_renamed = (df_counts.withColumn('dimension', F.lit(dim))
                  .withColumnRenamed(dim, 'value')
                  )
    pdf = df_renamed.orderBy('count', ascending=False).toPandas()
    return pdf[['dimension', 'value', 'count', 'unique', 'duplicates']].set_index('dimension')


def reach_media_type(make_media, df, map_df, dims):
    first = True
    for d in dims:
        adf, name_pdf = create_friendly_df(make_media, df, map_df, d)
        dim_pdf = reach_count(adf, 'household_id', 'val_id', 'projfact')
        dim_pdf['value'] = dim_pdf.value.apply(lambda v: name_pdf.loc[v, 'name'])
        dim_pdf['dimension'] = d
        if first:
            pdf = dim_pdf
            first = False
        else:
            pdf = pdf.append(dim_pdf)
    return pdf


def make_overall_plus(make_media, media_list):
    # make overall reach
    df_overall = reach_count(make_media(media_list, 'date'),
                             'household_id', project='projfact')
    df_overall['dimension'] = 'Overall'
    df_overall['type'] = 'All'
    # make etype reach
    df_etypes = reach_count(make_media(media_list, 'etype'),
                            'household_id', 'etype', project='projfact')
    df_etypes['dimension'] = 'Event Type'
    df_etypes['type'] = 'All'
    return df_overall.append(df_etypes)


def main(make_media, media_list, mtypes, map_df, df_hier):
    reach_pdf = make_overall_plus(make_media, media_list)
    for m, df in zip(mtypes, media_list):
        dims = none_to_empty(get_hierarchy_from_df(df_hier, m))
        m_pdf = reach_media_type(make_media, df, map_df, dims)
        m_pdf['type'] = m
        reach_pdf = reach_pdf.append(m_pdf)
    reach_pdf[['type', 'dimension', 'value']] = dimension_to_friendlyname(
        reach_pdf[['type', 'dimension', 'value']], df_hier.toPandas())
    return reach_pdf[['type', 'dimension', 'value', 'count', 'unique', 'duplicates']]\
        .rename(columns=lambda v: v.title())
