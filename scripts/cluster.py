#!/usr/bin/env python3
"""Own and manage a localhost-only, two-worker Hadoop + Spark cluster."""
import argparse
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import sys
import time
from urllib.request import urlopen
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = Path(os.environ.get('YT_RUNTIME', Path.home() / '.cache/youtube-pulse'))
HADOOP = RUNTIME / 'hadoop-3.3.6'
PYTHON = RUNTIME / 'venv/bin/python'
STATE = RUNTIME / 'state'


def environment():
    env = os.environ.copy()
    if not env.get('JAVA_HOME') and sys.platform == 'darwin':
        env['JAVA_HOME'] = subprocess.check_output(['/usr/libexec/java_home', '-v', '11'], text=True).strip()
    env.update(HADOOP_HOME=str(HADOOP), HADOOP_CONF_DIR=str(STATE / 'conf/master'),
               HADOOP_LOG_DIR=str(STATE / 'logs'), HADOOP_HEAPSIZE_MAX='256',
               HADOOP_PID_DIR=str(STATE / 'pids/master'),
               HADOOP_OPTS='-XX:ActiveProcessorCount=2',
               SPARK_LOCAL_IP='127.0.0.1', SPARK_DAEMON_MEMORY='128m',
               PYSPARK_PYTHON=str(PYTHON), PYSPARK_DRIVER_PYTHON=str(PYTHON))
    return env


def spark_home():
    return Path(subprocess.check_output([str(PYTHON), '-c', 'import pyspark, pathlib; print(pathlib.Path(pyspark.__file__).parent)'], text=True).strip())


def xml_config(path, values):
    path.write_text('<?xml version="1.0"?>\n<configuration>\n' + ''.join(
        f'  <property><name>{escape(k)}</name><value>{escape(str(v))}</value></property>\n'
        for k, v in values.items()) + '</configuration>\n')


def configure():
    for directory in ['logs', 'pids', 'namenode', 'datanode-1', 'datanode-2', 'spark-1', 'spark-2']:
        (STATE / directory).mkdir(parents=True, exist_ok=True)
    for node in ['master', 'worker1', 'worker2']:
        conf = STATE / 'conf' / node
        conf.mkdir(parents=True, exist_ok=True)
        (STATE / 'pids' / node).mkdir(parents=True, exist_ok=True)
        # Hadoop treats a terminated-but-not-yet-reaped process as alive via kill -0.
        for pidfile in (STATE / 'pids' / node).glob('hadoop-*.pid'):
            try:
                pid = int(pidfile.read_text().strip())
                process_state = subprocess.run(['ps', '-p', str(pid), '-o', 'stat='], capture_output=True, text=True).stdout.strip()
                if not process_state or process_state.startswith('Z'):
                    pidfile.unlink()
            except ValueError:
                raise RuntimeError(f'Invalid Hadoop PID file: {pidfile}')
        shutil.copy2(HADOOP / 'etc/hadoop/log4j.properties', conf / 'log4j.properties')
        xml_config(conf / 'core-site.xml', {'fs.defaultFS': 'hdfs://127.0.0.1:9000',
                                           'hadoop.tmp.dir': str(STATE / 'tmp' / node)})
        values = {'dfs.replication': 2, 'dfs.blocksize': 4 * 1024 * 1024,
                  'dfs.namenode.name.dir': (STATE / 'namenode').as_uri(),
                  'dfs.namenode.rpc-address': '127.0.0.1:9000',
                  'dfs.namenode.http-address': '127.0.0.1:9870',
                  'dfs.namenode.safemode.min.datanodes': 2,
                  'dfs.namenode.safemode.extension': 1000,
                  'dfs.client.use.datanode.hostname': 'false'}
        if node != 'master':
            n = int(node[-1])
            base = 9860 if n == 1 else 9880
            values.update({'dfs.datanode.data.dir': (STATE / f'datanode-{n}').as_uri(),
                           'dfs.datanode.address': f'127.0.0.1:{base + 6}',
                           'dfs.datanode.ipc.address': f'127.0.0.1:{base + 7}',
                           'dfs.datanode.http.address': f'127.0.0.1:{base + 4}',
                           'dfs.datanode.hostname': '127.0.0.1'})
        xml_config(conf / 'hdfs-site.xml', values)


def hdfs(*args, **kwargs):
    return subprocess.run([str(HADOOP / 'bin/hdfs'), *args], env=environment(), check=True, **kwargs)


def alive(name):
    path = STATE / 'pids' / f'{name}.json'
    if not path.exists():
        return False
    record = json.loads(path.read_text())
    try:
        command = subprocess.check_output(['ps', '-p', str(record['pid']), '-o', 'command='], text=True).strip()
        return record['marker'] in command
    except subprocess.CalledProcessError:
        return False


def launch(name, command, marker, env=None):
    if alive(name):
        print(f'{name}: already running')
        return
    with (STATE / 'logs' / f'{name}.log').open('a') as log:
        proc = subprocess.Popen(command, env=env or environment(), stdout=log, stderr=log,
                                start_new_session=True, cwd=ROOT)
    (STATE / 'pids' / f'{name}.json').write_text(json.dumps({'pid': proc.pid, 'marker': marker}))
    for _ in range(100):
        if proc.poll() is not None:
            raise RuntimeError(f'{name} exited; inspect {STATE / "logs" / (name + ".log")}')
        if alive(name):
            break
        time.sleep(0.1)
    else:
        raise RuntimeError(f'{name} did not finish launching; inspect its log')
    print(f'{name}: started pid {proc.pid}', flush=True)


def get_json(url):
    with urlopen(url, timeout=3) as response:
        return json.load(response)


def status():
    result = {'mode': 'single-host, two logical workers', 'hdfs_live_datanodes': 0,
              'spark_alive_workers': 0, 'spark_workers': [], 'errors': []}
    try:
        bean = get_json('http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo')['beans'][0]
        result['hdfs_live_datanodes'] = len(json.loads(bean['LiveNodes']))
    except Exception as exc:
        result['errors'].append(f'HDFS: {exc}')
    try:
        master = get_json('http://127.0.0.1:8080/json/')
        workers = [w for w in master['workers'] if w['state'] == 'ALIVE']
        result['spark_alive_workers'] = len(workers)
        result['spark_workers'] = workers
    except Exception as exc:
        result['errors'].append(f'Spark: {exc}')
    return result


def start():
    configure()
    # Never reformat an existing filesystem.
    if not (STATE / 'namenode/current/VERSION').exists():
        hdfs('namenode', '-format', '-nonInteractive')
    launch('namenode', [str(HADOOP / 'bin/hdfs'), 'namenode'], 'org.apache.hadoop.hdfs.server.namenode.NameNode')
    for n in [1, 2]:
        env = environment()
        env['HADOOP_CONF_DIR'] = str(STATE / f'conf/worker{n}')
        env['HADOOP_PID_DIR'] = str(STATE / f'pids/worker{n}')
        launch(f'datanode-{n}', [str(HADOOP / 'bin/hdfs'), 'datanode'], 'org.apache.hadoop.hdfs.server.datanode.DataNode', env)
    spark = spark_home()
    master_class = 'org.apache.spark.deploy.master.Master'
    launch('spark-master', [str(spark / 'bin/spark-class'), master_class, '--host', '127.0.0.1',
                            '--port', '7077', '--webui-port', '8080'], master_class)
    for n in [1, 2]:
        worker_class = 'org.apache.spark.deploy.worker.Worker'
        launch(f'spark-worker-{n}', [str(spark / 'bin/spark-class'), worker_class,
               '--host', '127.0.0.1', '--port', str(7077+n), '--webui-port', str(8080+n),
               '--cores', '1', '--memory', '768m', '--work-dir', str(STATE / f'spark-{n}'),
               'spark://127.0.0.1:7077'], worker_class)
    for _ in range(45):
        current = status()
        if current['hdfs_live_datanodes'] == 2 and current['spark_alive_workers'] == 2:
            hdfs('dfsadmin', '-safemode', 'wait')
            print(json.dumps(current, indent=2))
            return
        time.sleep(2)
    raise SystemExit(f'Cluster not ready. Inspect logs at {STATE / "logs"}')


def stop():
    for name in ['dashboard', 'spark-worker-2', 'spark-worker-1', 'spark-master', 'datanode-2', 'datanode-1', 'namenode']:
        path = STATE / 'pids' / f'{name}.json'
        if alive(name):
            pid = json.loads(path.read_text())['pid']
            os.killpg(pid, signal.SIGTERM)
            for _ in range(50):
                if not alive(name):
                    break
                time.sleep(0.1)
            if alive(name):
                raise RuntimeError(f'{name} is still stopping; retry stop before restarting')
            print(f'Stopped {name}')
        path.unlink(missing_ok=True)


def dashboard():
    configure()
    launch('dashboard', [str(PYTHON), '-m', 'streamlit', 'run', str(ROOT / 'dashboard/app.py'),
                        '--server.address', '127.0.0.1', '--server.port', '8501',
                        '--server.headless', 'true', '--browser.gatherUsageStats', 'false'], 'streamlit')
    for _ in range(30):
        if not alive('dashboard'):
            raise RuntimeError('Dashboard exited. Check port 8501 and dashboard.log.')
        try:
            with urlopen('http://127.0.0.1:8501/_stcore/health', timeout=1) as response:
                if response.status == 200:
                    break
        except OSError:
            time.sleep(0.5)
    else:
        raise RuntimeError('Dashboard did not become ready; inspect dashboard.log')
    print('Dashboard: http://127.0.0.1:8501')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'stop', 'status', 'dashboard', 'hdfs'])
    args, extra = parser.parse_known_args()
    if args.action == 'hdfs':
        hdfs(*extra)
    elif args.action == 'status':
        print(json.dumps(status(), indent=2))
    else:
        globals()[args.action]()
