import pandas as pd
from dataExceptions import DataException
from dailyConnection import conn


def check_tables(dic):
    """
    check tables to make sure log table exists
    Also output message if adServer table doesn't exist
    """
    sql_str = """select distinct tablename
    from pg_table_def
    where schemaname = '{s}'
    """.format(s=dic['schema'])
    tables = pd.read_sql(sql_str, conn).tablename.values
    if dic['ad_tab'] not in tables:
        dic['ad_tab'] = None
    if dic['log_tab'] not in tables:
        missing_log(dic)


def memoize(func):
    memo = {}

    def helper(x):
        if x not in memo:
            memo[x] = func(x)
        return memo[x]
    return helper


def check_timestamp(schema, table):
    sql_str = """select type from pg_table_def
    where schemaname = '{s}' and tablename = '{t}'
        and "column" = 'timestamp' """.format(s=schema, t=table)
    val = pd.read_sql(sql_str, conn).iloc[0, 0]
    if 'timestamp' not in val:
        err_msg = """Time shift requested or date column missing but timestamp \
column not in timestamp format. Timestamp column format is {}""".format(val)
        raise DataException(err_msg)


def shift_hours(tab, hours, tabcols):
    """
    Creates temporary table to shift log table hours according to request.
    Will also raise exception if timestamp column not present
    """
    colstr = '", "'.join(c for c in tabcols if c != 'date')
    if 'exe_imp' not in tabcols:
        add_str = ', 1 as exe_imp'
    else:
        add_str = ''
    sqlstr = """create temp table {t}_shift as \
    select "{cs}", trunc(date_add('h', {h}, "timestamp")) as "date"{ad}
    from {t} """.format(h=hours, t=tab, cs=colstr, ad=add_str)
    return [sqlstr]


def group_string(table, dims, aggstr, where):
    """
    Creates the sql script to create pandas dataframe
    """
    if not is_str(dims):
        dimstr = ', '.join(dims)
    else:
        dimstr = dims
    if len(where) > 0:
        where = ' where ' + where
    return """select {d}, {a}
    from {t} {w} group by {d} """.format(d=dimstr, a=aggstr, t=table, w=where)


def join_dfs(conn, log_tab, ad_tab, dim, where=''):
    """
    Joins log tab with ad tab if ad tab present according to dimension
    Also produces diff/pct_diff metrics
    """
    lstr = group_string(log_tab, dim, 'sum(exe_imp) as exe_imp', where)
    log_df_g = pd.read_sql(lstr, conn)
    if ad_tab is None:
        return log_df_g
    astr = group_string(ad_tab, dim, 'sum(impressions) as impressions', where)
    ad_df_g = pd.read_sql(astr, conn)
    j_df = log_df_g.merge(ad_df_g, on=dim, how='outer')
    j_df['diff'] = j_df.impressions - j_df.exe_imp
    j_df['pct_diff'] = j_df['diff'] / j_df.impressions
    return j_df


# def list_to_string(pdf, dim):
#     if isinstance(dim, basestring):
#         return pdf.rename(columns={dim: 'Value'})
#     pdf['Value'] = pdf[dim].apply(lambda r: ' '.join(r), axis=1)
#     return pdf.drop(dim, axis=1)


def reorder_cols(pdf, firstcols=('Dimension', 'adserver', 'configurationid', 'date',
                                 'campaignid', 'siteid', 'placementid', 'creativeid')):
    fcols2 = [c for c in firstcols if c in pdf.columns]
    rest = [c for c in pdf.columns if c not in fcols2]
    return pdf[fcols2 + rest]


def rename_cols(pdf):
    renames = {'diff': 'Difference', 'pct_diff': r'% Difference', 'date': 'Date',
               'exe_imp': 'Log Rows', 'impressions': 'AdServer Imps.', 'adserver': 'adServer'}
    return pdf.rename(columns=renames)


def missing_log(dic):
    """
    raise exception when log table does not exist
    """
    message = """Log table {t} does not exist. \
Cannot perform daily eda.""".format(t=dic['log_tab'])
    if dic['ad_tab'] is None:
        message += """\nadServer table {t} also does not exist.
However adServer table is not required to run daily eda."""\
        .format(t=dic['log_tab'])
    raise DataException(message)


def list_helper(l, alist):
    """
    Checks if l and all elements in l belong to alist
    """
    if is_str(l):
        return l in alist
    elif len(l) == 1:
        return list_helper(l[0], alist)
    return list_helper(l[0], alist) and list_helper(l[1:], alist)


# def missing_col(c):
#     return pd.DataFrame({'Error': ['Column {} does not exist in log table'.format(c)]})


def add_date(pdf, w, p):
    if w == '' and 'date' not in pdf.columns:
        pdf['date'] = 'whole period'
    elif 'date' not in pdf.columns:
        pdf['date'] = '{} days'.format(p)
    return pdf


def add_dimension(pdf, d):
    if is_str(d):
        pdf['Dimension'] = d
    else:
        pdf['Dimension'] = ' '.join(d)
    return pdf


def is_str(val):
    return isinstance(val, basestring)


def agg_to_list_str(series):
    """
    custom agg function for taking series and outputting to one string of unique values
    """
    aset = set(s.strip() for s in series)
    return ' '.join(sorted(aset))


def make_meta(conn, tab, meta_d, d, w):
    """
    make meta pdf for either/both log tab and ad tab for merge into final pdf
    """
    if len(meta_d) > 0:
        meta_str = ', ' + ', '.join(meta_d)
    else:
        meta_str = ''
    if is_str(d):
        d_str = d
    else:
        d_str = ', '.join(d)
    if len(w) > 0:
        where = 'where ' + w
    else:
        where = ''
    sql_str = ("""select distinct {d} {m} from {t} {w}"""
               .format(d=d_str, m=meta_str, t=tab, w=where))
    pdf = pd.read_sql(sql_str, conn)
    if len(meta_d) > 0 and pdf.shape[0] > 1:
        pdf = pdf.groupby(d, as_index=False).agg(agg_to_list_str)
    return pdf
