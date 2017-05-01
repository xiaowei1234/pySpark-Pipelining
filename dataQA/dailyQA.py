import pandas as pd
from dailyQAHelpers import *
from dailyConnection import conn
from dataExceptions import DataException
from dailyLogger import logger


class DailyQA(object):
    """
    """
    _configs = {'hourshift': 0, 'period': 7,
                'cols': ['campaignid', 'siteid', 'placementid', 'creativeid',
                         ['placementid', 'creativeid']],
                'meta_log': ['configurationid'], 'meta_ad': ['adserver']
                }
    _execution = []
    colstr = """select "column" from pg_table_def \
where schemaname = '{s}' and tablename = '{t}'"""

    def __init__(self, **kwargs):
        self._configs.update(kwargs)
        self.check_cols()
        if self.configs['hourshift'] != 0 or not list_helper(['date', 'exe_imp'], self.log_cols):
            self._execution += shift_hours(self.log_tab, self.configs['hourshift'], self.log_cols)
            self._configs['log_tab'] = self.log_tab + '_shift'

    @property
    def configs(self):
        return dict(self._configs)

    @property
    def log_tab(self):
        return self.configs['log_tab']

    @property
    def ad_tab(self):
        return self.configs['ad_tab']

    @property
    def cols(self):
        return list(self._configs['cols'])

    @property
    @memoize
    def min_max_dt(self):
        max_d = pd.read_sql("""select max("date") from {t}"""
                            .format(t=self.log_tab), conn).iloc[0, 0]
        min_d = max_d - pd.DateOffset(days=self.configs['period'] - 1)
        return (""""date" between '{min}' and '{max}' """
                .format(min=str(min_d)[:10], max=str(max_d)[:10]))

    @property
    def wheres(self):
        yield ''
        yield self.min_max_dt
        yield self.min_max_dt

    @property
    def groupings(self):
        yield self.cols + ['date']
        yield self.cols
        yield [list([c, 'date']) if is_str(c) else c + ['date']
               for c in self.cols]

    @property
    def log_cols(self):
        sql_str = ("""select * from {t} limit 0"""
                   .format(t=self.log_tab))
        cs = pd.read_sql(sql_str, conn).columns
        return cs

    @property
    @memoize
    def ad_cols(self):
        d = self.configs
        if self.ad_tab is None:
            logger.warn('adServer table {} does not exist'.format(self.ad_tab))
            return []
        sql_str = self.colstr.format(s=d['schema'], t=self.ad_tab)
        return pd.read_sql(sql_str, conn)['column'].values

    @property
    def execution(self):
        for c in self._execution:
            yield c

    def create_groupings(self):
        pdf_list = []
        for g, w in zip(self.groupings, self.wheres):
            for d in g:
                if not list_helper(d, self.log_cols):
                    logger.warn('At least one column {cs} not in log table {t}'
                                .format(cs=d, t=self.log_tab))
                    continue
                meta_pdf = make_meta(conn, self.log_tab, self.log_meta, d, w)
                if self.ad_tab is not None and list_helper(d, self.ad_cols):
                    pdf = join_dfs(conn, self.log_tab, self.ad_tab, d, w)
                    meta_ad = make_meta(conn, self.ad_tab, self.ad_meta(d), d, w)
                    meta_pdf = meta_pdf.merge(meta_ad, on=d, how='outer')
                else:
                    if self.ad_tab is not None:
                        logger.warn('Dimension {} not found in adServer columns'.format(d))
                    pdf = join_dfs(conn, self.log_tab, None, d, w)
                pdf_meta = pdf.merge(meta_pdf, on=d, how='left')
                pdf_norm = (pdf_meta
                            .pipe(add_date, w, self.configs['period'])
                            .pipe(add_dimension, d)
                            )
                pdf_list.append(pdf_norm)
            logger.info('groupings: {} finished'.format(len(pdf_list)))
        pdf_all = pd.concat(pdf_list, ignore_index=True)
        return pdf_all.pipe(reorder_cols).pipe(rename_cols)

    def ad_meta(self, d):
        """
        return columns needed for adserver meta pdf
        """
        names = []
        if is_str(d):
            aname = d[:-2] + 'name'
            if aname in self.ad_cols:
                names = [aname]
        else:
            names = [n[:-2] + 'name' for n in d if n[:-2] + 'name' in self.ad_cols]
        return self.configs['meta_ad'] + names

    @property
    def log_meta(self):
        return [d for d in self.configs['meta_log'] if d in self.log_cols]

    def check_cols(self):
        """
        Check if requisite date or timestamp columns exist
        """
        if self.ad_tab is not None and 'date' not in self.ad_cols:
            raise DataException("""date column not found in adServer table.""")
        if self.ad_tab is not None and 'impressions' not in self.ad_cols:
            raise DataException("""impressions column not found in adServer table.""")
        if 'timestamp' not in self.log_cols and 'date' not in self.log_cols:
            raise DataException("""Both timestamp and date column missing from {t}
Cannot do dailyQA""".format(t=self.log_tab))
        if self.configs['hourshift'] != 0 or 'date' not in self.log_cols:
            if 'timestamp' not in self.log_cols:
                raise DataException("""Time shift requested \
but no timestamp column in {t}.""".format(t=self.log_tab))
            else:
                check_timestamp(self.configs['schema'], self.log_tab)
