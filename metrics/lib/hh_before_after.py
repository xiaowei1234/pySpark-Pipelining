import pyspark.sql.functions as F
from metrics.helpers.make_media import union_df_list


def construct_hh_first(purch_df, expo_dfs, univ_df):
    dfs = [d.select('household_id', 'date') for d in expo_dfs]
    df = union_df_list(dfs)
    joined_df = (df.groupBy('household_id')
                 .agg(F.min('date').alias('date'))
                 .join(univ_df, on='household_id', how='leftsemi')
                 .join(purch_df, on='household_id', how='outer')
                 )
    max_dt = purch_df.groupBy().agg(F.max('pdate')).collect()[0][0]
    min_dt = df.groupBy().agg(F.min('date')).collect()[0][0]
    null_dates = F.col('date').isNull()
    joined_df2 = (joined_df.withColumn('max_dt', F.lit(max_dt))
                  .withColumn('hh_exposed', F.when(null_dates, 'not exposed')
                              .otherwise('exposed'))
                  .withColumn('date', F.when(null_dates, min_dt).otherwise(F.col('date')))
                  )
    return joined_df2.repartition(100, 'household_id')


def get_only_hh(df_left, df_right):
    right1 = df_right.select('hh_exposed', 'household_id').withColumn('right', F.lit(1))
    df_subtracted = (df_left
                     .join(right1, on=['hh_exposed', 'household_id'], how='left_outer')
                     .where(F.col('right').isNull())
                     .withColumn('volume_tgt', F.lit(0.0))
                     .withColumn('volume_cat', F.lit(0.0))
                     .drop('right')
                     )
    return df_subtracted


def add_zero_purch(df):
    df_before = df.filter(df.bef_aft == 'before')
    df_after = df.filter(df.bef_aft == 'after')
    df_union_after = get_only_hh(df_before, df_after).withColumn('bef_aft', F.lit('after'))
    df_union_before = get_only_hh(df_after, df_before).withColumn('bef_aft', F.lit('before'))
    return (df.unionAll(df_union_after.select(*df.columns))
            .unionAll(df_union_before.select(*df.columns))
            )


def create_before_after(df):
    return df.withColumn('bef_aft', F.when(
        F.col('pdate') < F.col('date'), 'before').otherwise('after'))


def agg_by_hh(df):
    aggregates = [F.max('wgt').alias('wgt'), F.sum('volume_tgt').alias('volume_tgt'),
                  F.sum('volume_cat').alias('volume_cat')]
    df_agg = df.groupBy('household_id', 'bef_aft', 'hh_exposed').agg(*aggregates)
    return df_agg


def filter_to_range(df, min_wks=1, max_wks=13):
    """
    returns dataframe filtered to necessary weeks
    df (param): spark dataframe that contains p_dt, cal_dt, and max_dt columns
    min_wks (param): minimum # of weeks before max_dt to be included
    max_wks (param): maximum # of weeks in either direction
    """
    post_week_max = F.date_add(F.col('date'), max_wks * 7 - 1)
    post_limit = F.when(
        F.col('max_dt') < post_week_max, F.col('max_dt')).otherwise(post_week_max)
    num_days_post = F.datediff(post_limit, F.col('date')) + 1
    min_wk_bool = num_days_post >= min_wks * 7
    df_filter = df.filter((F.datediff(df.date, df.pdate) < num_days_post) &
                          (df.pdate <= post_limit) &
                          min_wk_bool
                          )
    return df_filter.withColumn('wgt', num_days_post)


def add_only_bb_hh(df):
    tgt_hh = (df.where(df.volume_tgt > 0).select('household_id').distinct()
              .withColumn('brandbuyer', F.lit(1))
              )
    df2 = (df.join(tgt_hh, on='household_id', how='left_outer')
           .fillna(0, subset=['brandbuyer'])
           .withColumn('volume_cat_bb', df.volume_cat * F.col('brandbuyer'))
           .drop('brandbuyer')
           )
    return df2


def add_weights(df):
    df_add = add_only_bb_hh(df)
    df2 = (df_add.withColumn('brand_wgt', F.col('volume_tgt') * F.col('wgt'))
           .withColumn('category_wgt', F.col('volume_cat') * F.col('wgt'))
           .withColumn('category_wgt_bb', F.col('volume_cat_bb') * F.col('wgt'))
           .withColumn('bwgt', F.when(df_add['volume_tgt'] > 0, df_add.wgt).otherwise(None))
           .withColumn('cwgt', F.when(df_add['volume_cat'] > 0, df_add.wgt).otherwise(None))
           .withColumn('cwgt_bb', F.when(df_add['volume_cat_bb'] > 0, df_add.wgt).otherwise(None))
           )
    return df2


def make_counts(df):
    to_agg = [F.sum(c).alias(c) for c in ['volume_tgt', 'volume_cat']]
    df_hh = (df.groupBy('bef_aft', 'household_id')
             .agg(*to_agg).fillna(0, ['volume_tgt', 'volume_cat'])
             )
    df_pv = df_hh.groupBy('household_id').pivot('bef_aft').sum()
    bb = F.col('before_sum(volume_tgt)')
    bc = F.col('before_sum(volume_cat)')
    ab = F.col('after_sum(volume_tgt)')
    ac = F.col('after_sum(volume_cat)')
    df_01 = (df_pv.withColumn('bb', F.when(bb > 0, 1).otherwise(0))
             .withColumn('bc', F.when(bc > bb, 1).otherwise(0))
             .withColumn('ab', F.when(ab > 0, 1).otherwise(0))
             .withColumn('ac', F.when(ac > ab, 1).otherwise(0))
             )
    df_grouped = df_01.groupBy(['bb', 'bc', 'ab', 'ac']).count()
    return df_grouped.toPandas()


def sum_bef_aft(df, cols):
    def count_non_null(c):
        return F.sum(F.col(c).isNotNull().cast('integer')).alias(c + '_cnt')
    to_count = [count_non_null(c) for c in cols if c in ['bwgt', 'cwgt', 'cwgt_bb']]
    to_agg = [F.sum(c).alias(c) for c in cols]
    to_agg = to_agg + to_count
    grouped_pdf = df.groupBy('bef_aft', 'hh_exposed').agg(*to_agg).toPandas()
    return grouped_pdf


def calc_metrics(pdf):
    pdf['ratio'] = pdf['volume_tgt'] / pdf['volume_cat']
    pdf['ratio_bb'] = pdf.volume_tgt / pdf.volume_cat_bb
    pdf['brand_wgt'] /= pdf['bwgt'] / pdf.bwgt_cnt
    pdf['category_wgt'] /= pdf['cwgt'] / pdf.cwgt_cnt
    pdf['category_wgt_bb'] /= pdf['cwgt_bb'] / pdf.cwgt_bb_cnt
    pdf['weighted_ratio'] = pdf['brand_wgt'] / pdf['category_wgt']
    pdf['weighted_ratio_bb'] = pdf.brand_wgt / pdf.category_wgt_bb
    pdf['i_ratio'] = pdf.bwgt_cnt / pdf.cwgt_cnt
    pdf['i_ratio_bb'] = pdf.bwgt_cnt / pdf.cwgt_bb_cnt
    return pdf


def get_before_after(df, minwks, maxwks):
    df_filtered = filter_to_range(df, minwks, maxwks)
    df_bef_af = create_before_after(df_filtered)
    df_hh_agg = agg_by_hh(df_bef_af)
    df_stable = add_zero_purch(df_hh_agg)
    df_weights = add_weights(df_stable)
    return df_weights


def make_before_after_metrics(df_weights):
    # brand vs category post exposure
    summed_pdf = sum_bef_aft(df_weights, ['volume_tgt', 'volume_cat', 'volume_cat_bb',
                             'bwgt', 'cwgt', 'cwgt_bb', 'brand_wgt', 'category_wgt',
                                          'category_wgt_bb', 'wgt'])
    ratio_pdf = calc_metrics(summed_pdf)
    return ratio_pdf


def main(purch_df, expo_dfs, univ_df):
    # raw table
    df_raw = construct_hh_first(purch_df, expo_dfs, univ_df)
    df_ratio = get_before_after(df_raw, 1, 13)
    rpdf = make_before_after_metrics(df_ratio)[
        ['hh_exposed', 'bef_aft', 'volume_tgt', 'volume_cat',
         'ratio', 'brand_wgt', 'category_wgt', 'weighted_ratio',
         'bwgt_cnt', 'cwgt_cnt', 'i_ratio', 'volume_cat_bb',
         'ratio_bb', 'category_wgt_bb', 'weighted_ratio_bb',
         'cwgt_bb_cnt', 'i_ratio_bb', 'wgt']
    ]
    rpdf.sort_values(['hh_exposed', 'bef_aft'], ascending=[True, False], inplace=True)
    df_splitc = get_before_after(
        df_raw.filter(F.col('hh_exposed') == 'exposed'), 6, 6)
    cpdf = make_counts(df_splitc)
    return rpdf, cpdf
