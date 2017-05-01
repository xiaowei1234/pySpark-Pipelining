import pyspark.sql.functions as F
from samo.lib.hierarchy import hierarchy_to_dataframe, get_samo_hierarchy


def union_df_list(df_list):
    df = df_list[0]
    for d in df_list[1:]:
        adf = d.select(*df.columns)
        df = df.unionAll(adf)
    return df


def get_hierarchy_from_df(df, dim):
    pdf = df.toPandas()
    dig_id = pdf.loc[pdf.dimension.apply(lambda v: v.lower()) == dim.lower(), 'id'].iloc[0]
    dims = pdf.loc[pdf.parent_id == dig_id, 'dimension'].apply(lambda v: v.lower())
    return list(dims)


def get_dimensions_from_configs(hc, mta_config, base_modeling_media):
    samo_hierarchy = get_samo_hierarchy(mta_config, base_modeling_media)
    df_samo_hierarchy = hierarchy_to_dataframe(hc, samo_hierarchy)
    return df_samo_hierarchy


def make_df_dec(proj_df, proj_df2=None):
    def make_df(df_list, dim):
        if not hasattr(df_list, '__iter__'):
            dfs = [df_list]
        else:
            dfs = df_list
        df_non = filter(lambda d: dim not in d.columns, dfs)
        if len(df_non) > 0:
            raise ValueError('dimension: {0} not found in at least one provided table'.format(dim))
        if proj_df2 is None or dim == 'etype':
            dfs = [d.select('household_id', dim) for d in dfs]
        else:
            dfs = [d.select('household_id', dim, 'etype') for d in dfs]
        df = union_df_list(dfs)
        df_joined = df.join(proj_df.dropDuplicates(['household_id']),
                            on='household_id', how='inner')
        if proj_df2 is not None:
           return (df_joined.withColumnRenamed('projfact', 'proj')
                   .join(proj_df2, 'etype').withColumn('projfact', F.col('proj') * F.col('ratio')))
        return df_joined
    return make_df
