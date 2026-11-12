#!/usr/bin/env python3
"""Upload real data, run Spark on both workers, export committed HDFS results."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
from cluster import ROOT, RUNTIME, STATE, PYTHON, hdfs, environment, spark_home, status

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--country', default='IN', choices=['IN', 'US', 'GB', 'CA', 'DE', 'FR'])
args = parser.parse_args()
current = status()
if current['hdfs_live_datanodes'] != 2 or current['spark_alive_workers'] != 2:
    raise SystemExit('Start the cluster first: .venv/bin/python scripts/cluster.py start')
for filename in [f'{args.country}videos.csv', f'{args.country}_category_id.json']:
    if not (ROOT / 'data/raw' / filename).exists():
        raise SystemExit(f'Missing {filename}; run scripts/download_data.py --country {args.country}')
hdfs('dfs', '-mkdir', '-p', '/youtube/raw', '/youtube/results', '/youtube/events')
for filename in [f'{args.country}videos.csv', f'{args.country}_category_id.json']:
    # Stream input so Hadoop's local path handling cannot break on spaces in the checkout path.
    with (ROOT / 'data/raw' / filename).open('rb') as source:
        hdfs('dfs', '-put', '-f', '-', f'/youtube/raw/{filename}', stdin=source)
hdfs('dfs', '-setrep', '-w', '2', '/youtube/raw')
artifacts = ROOT / 'artifacts'
artifacts.mkdir(exist_ok=True)
command = [str(spark_home() / 'bin/spark-submit'), '--master', 'spark://127.0.0.1:7077',
           '--deploy-mode', 'client', '--driver-memory', '768m', '--executor-memory', '512m',
           '--executor-cores', '1', '--total-executor-cores', '2',
           '--conf', 'spark.driver.host=127.0.0.1', '--conf', 'spark.driver.bindAddress=127.0.0.1',
           '--conf', 'spark.driver.extraJavaOptions=-XX:ActiveProcessorCount=2',
           '--conf', 'spark.executor.extraJavaOptions=-XX:ActiveProcessorCount=1',
           '--conf', 'spark.eventLog.enabled=true', '--conf', 'spark.eventLog.dir=hdfs://127.0.0.1:9000/youtube/events',
           str(ROOT / 'pipeline/analyze.py'), '--country', args.country,
           '--manifest', str(artifacts / 'run.json')]
with (artifacts / 'pipeline.log').open('w') as log:
    result = subprocess.run(command, env=environment(), stdout=log, stderr=subprocess.STDOUT)
if result.returncode:
    print((artifacts / 'pipeline.log').read_text()[-8000:])
    raise SystemExit('Spark failed; full log: artifacts/pipeline.log')
for dataset in ['snapshots', 'latest', 'daily', 'categories']:
    target = artifacts / f'{dataset}.jsonl'
    temp = target.with_suffix('.tmp')
    with temp.open('wb') as output:
        hdfs('dfs', '-cat', f'/youtube/results/{args.country}/{dataset}/part-*.json', stdout=output)
    temp.replace(target)
(artifacts / 'cluster.json').write_text(json.dumps(status(), indent=2))
with (artifacts / 'hdfs-report.txt').open('w') as output:
    hdfs('dfsadmin', '-report', stdout=output)
with (artifacts / 'hdfs-blocks.txt').open('w') as output:
    hdfs('fsck', '/youtube/raw', '-files', '-blocks', '-locations', stdout=output)
shutil.copy2(ROOT / f'data/raw/{args.country}_source.json', artifacts / 'source.json')
print((artifacts / 'run.json').read_text())
print('Complete. Start dashboard: .venv/bin/python scripts/cluster.py dashboard')
