import pyspark.sql.functions as F


def none_to_empty(alist):
    """
    returns empty list if alist is None otherwise return alist
    """
    return [] if alist is None else alist


def input_to_list(string):
    return [s for s in string.split(' ') if len(s) > 0]


def create_friendly_df(make_media, df, map_df, dim):
    """
    returns initial df for reach metrics with additional columns specified by keys_meta
    keys_meta (param): list of values or None used for reach found in
        keno_strs 'key' column in keno_meta and merged in using placementid
    """
    relevant_map_df = map_df.filter(map_df.key == dim)
    dim_row = relevant_map_df.select('dimension').take(1)
    if len(dim_row) == 0:
        raise ValueError('Dimension {0} does not exist in key column of mapping table'.format(dim))
    dimension = dim_row[0][0]
    df = make_media(df, dimension)
    relevant_map_df2 = (relevant_map_df.selectExpr('id as {}'.format(dimension), 'val_id')
                        .dropDuplicates([dimension]))
    df2 = df.join(relevant_map_df2, on=dimension, how='left_outer')
    name_map = (relevant_map_df.drop_duplicates(['val_id'])
                .withColumn('name', F.coalesce('value', 'label'))
                .select('val_id', 'name'))
    return df2, name_map.toPandas().set_index('val_id')


def samo_hier_df_map(pdf):
    apdf = pdf[['dimension', 'friendly_name']]
    apdf['dimension'] = apdf.dimension.apply(lambda v: v.lower())
    return apdf.drop_duplicates('dimension').set_index('dimension')


def dimension_to_friendlyname(pdf, samo_pdf):
    pdf_map = samo_hier_df_map(samo_pdf)
    for c in pdf.columns:
        pdf[c] = pdf[c].apply(
            lambda v: pdf_map.loc[v.lower(), 'friendly_name']
            if v in pdf_map.index else v)
    return pdf
