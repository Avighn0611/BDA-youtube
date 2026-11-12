"""YouTube batch analytics. All cleaning and analytical datasets are built in Spark."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

from pyspark.sql import SparkSession, Window, functions as F, types as T

CSV_COLUMNS = ['video_id', 'trending_date', 'title', 'channel_title', 'category_id', 'publish_time',
               'tags', 'views', 'likes', 'dislikes', 'comment_count', 'thumbnail_link',
               'comments_disabled', 'ratings_disabled', 'video_error_or_removed', 'description']


def clean(raw, country):
    df = raw.withColumn('country', F.lit(country)).withColumn('trending_date', F.to_date('trending_date', 'yy.dd.MM'))
    for column in ['views', 'likes', 'comment_count', 'category_id']:
        df = df.withColumn(column, F.col(column).cast('long'))
    df = df.withColumn('publish_time', F.to_timestamp('publish_time'))
    for column in ['comments_disabled', 'ratings_disabled', 'video_error_or_removed']:
        df = df.withColumn(column, F.lower(F.col(column)) == 'true')
    valid = F.col('video_id').rlike(r'^[A-Za-z0-9_-]{11}$') & F.col('trending_date').isNotNull()
    for column in ['views', 'likes', 'comment_count']:
        valid = valid & F.col(column).isNotNull() & (F.col(column) >= 0)
    df = df.filter(valid)
    daily_window = Window.partitionBy('country', 'video_id', 'trending_date').orderBy(
        F.desc('views'), F.desc('likes'), F.desc('comment_count'), F.asc('title'))
    df = df.withColumn('_row', F.row_number().over(daily_window)).filter('_row = 1').drop('_row')
    return df.withColumn('engagement_pct', F.when((F.col('views') > 0) & ~F.coalesce(F.col('ratings_disabled'), F.lit(False)),
                100.0 * (F.col('likes') + F.col('comment_count')) / F.col('views'))).withColumn(
                'days_to_trend', F.datediff('trending_date', F.to_date('publish_time')))


def latest_snapshot(df):
    window = Window.partitionBy('country', 'video_id').orderBy(F.desc('trending_date'), F.desc('views'))
    return df.withColumn('_row', F.row_number().over(window)).filter('_row = 1').drop('_row')


def aggregate_categories(df):
    return df.groupBy('country', 'category').agg(
        F.count('*').alias('videos'), F.sum('views').alias('observed_views'),
        F.sum('likes').alias('observed_likes'),
        F.expr('percentile_approx(engagement_pct, 0.5)').alias('median_engagement_pct'),
        F.expr('percentile_approx(views, 0.5)').alias('median_views'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--country', default='IN')
    parser.add_argument('--input', default='hdfs://127.0.0.1:9000/youtube/raw')
    parser.add_argument('--output', default='hdfs://127.0.0.1:9000/youtube/results')
    parser.add_argument('--manifest', required=True)
    args = parser.parse_args()
    began = time.monotonic()
    spark = (SparkSession.builder.appName('YouTube Pulse | Trending Analytics')
             .config('spark.sql.shuffle.partitions', '4')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
             .config('spark.sql.adaptive.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    schema = T.StructType([T.StructField(c, T.StringType()) for c in CSV_COLUMNS])
    raw = spark.read.schema(schema).options(header=True, multiLine=True, escape='"', mode='FAILFAST').csv(
        f'{args.input}/{args.country}videos.csv')
    raw_count = raw.count()
    # Multiline CSV is not splittable: redistribute after parsing so downstream analysis uses both executors.
    prepared = clean(raw.repartition(4), args.country)
    categories = spark.read.option('multiLine', True).json(f'{args.input}/{args.country}_category_id.json')
    categories = categories.select(F.explode('items').alias('item')).select(
        F.col('item.id').cast('long').alias('category_id'), F.col('item.snippet.title').alias('category'))
    selected = ['country', 'video_id', 'trending_date', 'title', 'channel_title', 'category',
                'publish_time', 'views', 'likes', 'comment_count', 'engagement_pct', 'days_to_trend',
                'comments_disabled', 'ratings_disabled', 'video_error_or_removed']
    snapshots = prepared.join(F.broadcast(categories), 'category_id', 'left').fillna({'category': 'Unknown'}).select(*selected).cache()
    snapshot_count = snapshots.count()
    latest = latest_snapshot(snapshots).cache()
    latest_count = latest.count()
    daily = snapshots.groupBy('country', 'category', 'trending_date').agg(
        F.count('*').alias('trending_videos'), F.sum('views').alias('observed_views'),
        F.expr('percentile_approx(engagement_pct, 0.5)').alias('median_engagement_pct'))
    output = f'{args.output}/{args.country}'
    snapshots.write.mode('overwrite').parquet(f'{output}/cleaned_parquet')
    for name, df in [('snapshots', snapshots), ('latest', latest), ('daily', daily), ('categories', aggregate_categories(latest))]:
        df.write.mode('overwrite').json(f'{output}/{name}')
    # Read back the persisted outputs, rather than only trusting the writer.
    persisted = spark.read.parquet(f'{output}/cleaned_parquet').count()
    if persisted != snapshot_count:
        raise RuntimeError('HDFS round-trip row count differs')
    dates = snapshots.agg(F.min('trending_date'), F.max('trending_date')).first()
    manifest = {'application_id': spark.sparkContext.applicationId, 'spark_master': spark.sparkContext.master,
                'generated_at': datetime.now(timezone.utc).isoformat(), 'country': args.country,
                'input_rows': raw_count, 'snapshot_rows': snapshot_count,
                'invalid_or_duplicate_rows_removed': raw_count - snapshot_count,
                'unique_videos': latest_count, 'first_date': str(dates[0]), 'last_date': str(dates[1]),
                'hdfs_output': output, 'hdfs_roundtrip_rows': persisted,
                'duration_seconds': round(time.monotonic() - began, 2),
                'deployment': 'single-host standalone cluster, two worker processes',
                'engagement_definition': '100 * (likes + comment_count) / views; null for zero views or disabled ratings',
                'deduplication': 'One row per country/video/date, highest view count wins; latest row per video for totals.',
                'limitations': ['Historical trending sample only.', 'Views are cumulative global video counters, not country-specific traffic.',
                               'Comments-disabled videos can have lower measured engagement.', 'No physical multi-machine deployment verified.']}
    Path(args.manifest).write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))
    latest.unpersist()
    snapshots.unpersist()
    spark.stop()


if __name__ == '__main__':
    main()
