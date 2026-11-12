import os
from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession, types as T

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.analyze import CSV_COLUMNS, clean, latest_snapshot


@pytest.fixture(scope='module')
def spark():
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    session = (SparkSession.builder.master('local[2]').appName('Analytics correctness tests')
               .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
               .config('spark.sql.legacy.timeParserPolicy', 'CORRECTED').getOrCreate())
    session.sparkContext.setLogLevel('ERROR')
    yield session
    session.stop()


def frame(spark, overrides):
    records = []
    for change in overrides:
        row = dict.fromkeys(CSV_COLUMNS, '')
        row.update(video_id='abcdefghijk', trending_date='17.14.11', title='Example', channel_title='Channel',
                   category_id='10', publish_time='2017-11-12T12:20:39.000Z', views='100', likes='10',
                   comment_count='2', comments_disabled='FALSE', ratings_disabled='FALSE', video_error_or_removed='FALSE')
        row.update(change)
        records.append(row)
    return spark.createDataFrame(records, T.StructType([T.StructField(c, T.StringType()) for c in CSV_COLUMNS]))


def test_daily_dedup_and_latest_do_not_sum_snapshots(spark):
    data = frame(spark, [{}, {'views': '150'}, {'trending_date': '17.15.11', 'views': '200'}])
    result = clean(data, 'IN')
    assert result.count() == 2
    latest = latest_snapshot(result).collect()
    assert len(latest) == 1
    assert latest[0].views == 200
    assert str(latest[0].trending_date) == '2017-11-15'


def test_invalid_ids_negative_counts_and_bad_dates_are_excluded(spark):
    data = frame(spark, [{}, {'video_id': '#NAME?'}, {'views': '-1'}, {'likes': 'oops'}, {'trending_date': 'bad'}])
    assert clean(data, 'IN').count() == 1


def test_engagement_zero_views_disabled_ratings_and_units(spark):
    data = frame(spark, [{}, {'video_id': 'bcdefghijkl', 'views': '0'}, {'video_id': 'cdefghijklm', 'ratings_disabled': 'TRUE'}])
    results = {row.video_id: row.engagement_pct for row in clean(data, 'IN').collect()}
    assert results['abcdefghijk'] == 12.0
    assert results['bcdefghijkl'] is None
    assert results['cdefghijklm'] is None


def test_latest_is_isolated_by_country(spark):
    india = clean(frame(spark, [{'views': '100'}]), 'IN')
    usa = clean(frame(spark, [{'views': '200'}]), 'US')
    assert latest_snapshot(india.unionByName(usa)).count() == 2
