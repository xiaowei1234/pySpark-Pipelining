import pandas as pd
from metrics.helpers.make_media import union_df_list
from metrics.helpers.name_mapping import dimension_to_friendlyname


def get_ratio(df, proj_df, proj_univ_df):
    df_proj = df.join(proj_df, on='household_id', how='inner')
    total = proj_univ_df.groupBy().sum('exposures').collect()[0][0]
    proj_num = df_proj.groupBy().sum('projfact').collect()[0][0]
    return total / proj_num


def get_total_ratio(df_list, proj_df, univ_list):
    new_df_list = [df.select('household_id') for df in df_list]
    df_hh = union_df_list(new_df_list)
    new_univ_list = [df.select('exposures') for df in univ_list]
    df_univ = union_df_list(new_univ_list)
    return get_ratio(df_hh, proj_df, df_univ)


def main(df_list, proj_df, univ_list, mtypes, df_hier):
    ratios = [get_ratio(d, proj_df, u) for d, u in zip(df_list, univ_list)]
    total_ratio = get_total_ratio(df_list, proj_df, univ_list)
    pdf = pd.DataFrame({'etype': mtypes + ['all'], 'ratio': ratios + [total_ratio]})
    pdf[['etype']] = dimension_to_friendlyname(pdf[['etype']], df_hier.toPandas())
    return pdf
