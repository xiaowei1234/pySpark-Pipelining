import pytest
from pyspark import SparkConf, SparkContext, HiveContext
from metrics.lib.hh_before_after import *
from pyspark.sql.types import StructType, StructField as sf, \
    LongType as lt, StringType as st, DateType as dt, DoubleType as db
import pandas as pd


@pytest.fixture(scope="session")
def spark_context():
    print "\nStarting SparkContext..."

    conf = SparkConf()
    conf.set("spark.app.name", "MPA tests")
    conf.set("spark.executor.instances", '4')
    conf.set("spark.executor.memory", "16g")
    conf.set("spark.executor.cores", "16")
    conf.set("spark.yarn.executor.memoryOverhead", "4000")
    conf.setMaster('yarn-client')

    sc = SparkContext.getOrCreate(conf=conf)

    print "Done\n"
    return sc


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def hive_context(spark_context):
    hc = HiveContext(spark_context)
    return hc


pytestmark = pytest.mark.lib

min_wks = 1
max_wks = 2


def to_dt(date_str):
    return pd.to_datetime(date_str).date()


@pytest.fixture(scope="module")
def hh_expo_df(hive_context):
    schema = StructType([
        sf('household_id', lt()),
        sf('volume_tgt', db()),
        sf('volume_cat', db()),
        sf('max_dt', dt()),
        sf('date', dt()),
        sf('pdate', dt())
    ])
    data = [
        # purchase after 2 weeks filtered out
        [1, 0.1, 2.0, to_dt('2017-01-28'), to_dt('2017-01-14'), to_dt('2017-01-28')],
        # purchase before 2 weeks filtered out
        [1, 0.5, 2.0, to_dt('2017-01-28'), to_dt('2017-01-14'), to_dt('2016-12-31')],
        # purchase within 2 weeks before
        [1, 0.2, 2.0, to_dt('2017-01-28'), to_dt('2017-01-14'), to_dt('2017-01-01')],
        # purchase within 2 weeks after
        [1, 0.3, 2.0, to_dt('2017-01-28'), to_dt('2017-01-14'), to_dt('2017-01-27')],
        # household not fulfill min wk threshold filtered out
        [2, 0.4, 2.0, to_dt('2017-01-28'), to_dt('2017-01-23'), to_dt('2017-01-27')],
        # household just fulfills min threshold
        [3, 0.6, 2.0, to_dt('2017-01-28'), to_dt('2017-01-22'), to_dt('2017-01-27')]
    ]
    return hive_context.createDataFrame(data, schema)


def test_after_out(hh_expo_df):
    df = filter_to_range(hh_expo_df, min_wks, max_wks)
    pdf = df.toPandas()
    assert pdf.loc[pdf.volume_tgt == 0.1, :].shape[0] == 0


def test_before_out(hh_expo_df):
    df = filter_to_range(hh_expo_df, min_wks, max_wks)
    pdf = df.toPandas()
    assert pdf.loc[pdf.volume_tgt == 0.5, :].shape[0] == 0


def test_before_in(hh_expo_df):
    df = filter_to_range(hh_expo_df, min_wks, max_wks)
    pdf = df.toPandas()
    assert pdf.loc[pdf.volume_tgt == 0.2, :].shape[0] == 1


def test_after_in(hh_expo_df):
    df = filter_to_range(hh_expo_df, min_wks, max_wks)
    pdf = df.toPandas()
    assert pdf.loc[pdf.volume_tgt == 0.3, :].shape[0] == 1


def test_min_out(hh_expo_df):
    df = filter_to_range(hh_expo_df, min_wks, max_wks)
    pdf = df.toPandas()
    assert pdf.loc[pdf.volume_tgt == 0.4, :].shape[0] == 0


def test_min_in(hh_expo_df):
    df = filter_to_range(hh_expo_df, min_wks, max_wks)
    pdf = df.toPandas()
    assert pdf.loc[pdf.volume_tgt == 0.6, :].shape[0] == 1
