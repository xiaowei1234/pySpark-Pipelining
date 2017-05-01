import pyspark.sql.functions as F


def filter_to_relevant(df):
    df2 = (df.withColumn('key', F.lower(df.key))
           .withColumn('id_type', F.regexp_replace('id_type', '_id', 'id'))
           )
    val_map = (df2.select('value').distinct()
               .withColumn('val_id', F.monotonically_increasing_id())
               )
    df3 = df2.join(val_map, on='value')
    return df3


def make_map(ka_df, dm_df):
    ka_df2 = filter_to_relevant(ka_df).withColumn('etype2', F.lit('dig'))
    join_cond = [dm_df.dimension == ka_df2.id_type, dm_df.label == ka_df2.id_value,
                 dm_df.etype == ka_df2.etype2]
    k_df2 = (ka_df2.join(dm_df, on=join_cond, how='right_outer')
             .withColumn('key', F.coalesce(F.col('key'), F.col('dimension')))
             .withColumn('val_id', F.coalesce('val_id', 'id'))
             )
    return k_df2.select('etype', 'key', 'dimension', 'label', 'id', 'value', 'val_id')
