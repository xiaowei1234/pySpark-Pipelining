from dailyQA import DailyQA
from dailyQAHelpers import check_tables
from dailyConnection import conn, execute
from dataExceptions import DataException, write_out
from dailyLogger import logger
from shutil import copy2
import os


def args_to_kwargs(args):
    """
    extra arguments to script are turned into configs here
    """
    dic = {'log_tab': 'log_daily', 'ad_tab': 'eda_adserver_whole'}
    keys = ['schema', 'filepath', 'hourshift', 'period', 'log_tab', 'ad_tab'][:len(args)]
    dic.update({k: v for k, v in zip(keys, args)})
    return dic


def main(args):
    logger.info('Starting daily QA csv generation process for {}'.format(args[0]))
    if len(args) >= 3:
        logger.info('Timeshift: {} hours.'.format(args[2]))
    DataException.schema = args[0]
    dic = args_to_kwargs(args)
    execute('set search_path to {}'.format(dic['schema']))
    try:
        check_tables(dic)
        qa = DailyQA(**dic)
        for command in qa.execution:
            execute(command)
        pdf = qa.create_groupings()
        pdf.to_csv(args[1], encoding='utf-8', index=False)
        logger.info('{}: daily QA completed succesfully'.format(args[0]))
    except DataException as e:
        logger.error(e)
        write_out(e, args[1])
    except Exception as e:
        logger.error(e)
        write_out(e, args[1])
    finally:
        conn.close()
        to_folder = os.path.dirname(args[1])
        if len(to_folder) > 0:
            copy2('qa_log.txt', to_folder)
