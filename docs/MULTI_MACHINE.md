# Moving to three physical machines

This is a deployment guide, not a claim that a physical deployment has been tested. The provided `cluster.py` launcher intentionally starts all services on loopback on a single host. Do not simply run it unchanged on three PCs: that creates three unrelated local clusters.

## Target placement

| Host | Services |
|---|---|
| Master PC | NameNode, Spark master, Python driver, dashboard |
| Worker PC 1 | DataNode, Spark worker/executor |
| Worker PC 2 | DataNode, Spark worker/executor |

1. Connect the machines to one trusted LAN. Confirm client isolation is disabled and every machine can reach the others. Reserve addresses or use stable resolvable hostnames.
2. Install matching Java 11, Hadoop 3.3.6, Spark 3.5.7, and Python 3.11 environments. Use native binaries appropriate to each CPU architecture. On Windows, select a supported Linux environment or adapt service startup rather than assuming Mac scripts run unchanged.
3. Generate separate Hadoop configuration directories on each host. Set `fs.defaultFS` and `dfs.namenode.rpc-address` to `hdfs://MASTER_IP:9000` and `MASTER_IP:9000` respectively (the latter has no URI prefix). Set NameNode HTTP address to `MASTER_IP:9870`.
4. Use distinct **local** storage directories on each worker. Set each DataNode's advertised hostname to its reachable worker IP/hostname and bind its transfer/IPC/HTTP services to a suitable interface. Default worker ports 9866/9867/9864 may be reused on different machines.
5. Format only a new NameNode once. Do not copy existing local-demo DataNode state into a newly formatted cluster. Start NameNode, then both DataNodes; verify two registered nodes and replication factor 2.
6. Start Spark master with `--host MASTER_IP --port 7077`. Start a worker on each worker machine with its own reachable `--host`, one or more cores, an appropriate memory limit, and `spark://MASTER_IP:7077` as master URL.
7. Run `spark-submit` on the master in **client** deploy mode. Set `spark.driver.host=MASTER_IP` and a reachable bind address. Fix `spark.driver.port` and `spark.blockManager.port` if firewall rules require predictable ports; allow worker-to-driver and worker-to-worker traffic. Set the Python interpreter path separately for each machine's environment; identical absolute paths are simplest.
8. Upload the source files into the new HDFS cluster. The existing analysis program supports `--input hdfs://MASTER_IP:9000/youtube/raw` and `--output hdfs://MASTER_IP:9000/youtube/results`; use the physical master's Spark URL in `spark-submit`. Adapt the orchestration script's loopback endpoints and dashboard status endpoints to match.
9. Export the completed HDFS JSON results to the master and run the dashboard there. Bind Streamlit to the LAN interface only if other PCs need browser access.
10. Verify task events on both physical workers, two DataNode locations for blocks, and correct dashboard results. Save screenshots showing distinct host addresses and rerun result verification.

Use private LAN connectivity for the classroom demo; these configurations do not include production authentication. Do not expose the unauthenticated cluster directly to the public internet.

## Required proof

- NameNode lists two live DataNodes on distinct physical host addresses.
- Spark master lists two workers on those addresses.
- Application event logs show completed tasks on both executors.
- HDFS block locations show replication across the workers.
- Spark outputs and independently checked dashboard values agree.

Networking and OS compatibility must be tested on the actual machines before claiming compliance with the three-PC requirement.
