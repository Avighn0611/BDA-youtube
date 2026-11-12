"""Prove completed tasks on multiple executors from the actual Spark event log."""
from collections import Counter
import json
import subprocess
from cluster import ROOT, HADOOP, environment

artifacts = ROOT / 'artifacts'
run = json.loads((artifacts / 'run.json').read_text())
event_path = f"/youtube/events/{run['application_id']}"
with (artifacts / 'spark-events.jsonl').open('wb') as out:
    subprocess.run([str(HADOOP / 'bin/hdfs'), 'dfs', '-cat', event_path], env=environment(), stdout=out, check=True)
executors = {}
completed = Counter()
failed = Counter()
with (artifacts / 'spark-events.jsonl').open() as source:
    for line in source:
        event = json.loads(line)
        if event['Event'] == 'SparkListenerExecutorAdded':
            executors[event['Executor ID']] = event['Executor Info']
        if event['Event'] == 'SparkListenerTaskEnd':
            executor = event['Task Info']['Executor ID']
            if event.get('Task End Reason', {}).get('Reason') == 'Success':
                completed[executor] += 1
            else:
                failed[executor] += 1
result = {'application_id': run['application_id'], 'event_log': f'hdfs://127.0.0.1:9000{event_path}',
          'executors': executors, 'completed_tasks_per_executor': dict(completed),
          'failed_tasks_per_executor': dict(failed),
          'two_executors_completed_tasks': len([key for key, count in completed.items() if key != 'driver' and count > 0]) >= 2,
          'physical_hosts': 1}
(artifacts / 'execution-evidence.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
if not result['two_executors_completed_tasks']:
    raise SystemExit('Two-executor execution was not demonstrated; inspect scheduling and event logs.')
