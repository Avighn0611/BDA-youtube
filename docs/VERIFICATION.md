# Verified local demonstration

Verified on 12 November 2026 (Asia/Kolkata), on an Apple-silicon Mac with 8 GB RAM.

| Check | Observed result |
|---|---|
| Hadoop archive | Apache SHA-512 checksum matched |
| Live HDFS DataNodes | 2 separate processes/storage directories |
| Live Spark workers | 2 separate processes, 1 core each |
| Input | Original Kaggle INvideos.csv + India category JSON |
| Raw rows | 37,352 |
| Valid unique daily snapshots | 32,265 |
| Unique video IDs | 16,305 |
| HDFS Parquet read-back | 32,265 rows matched |
| Spark application | `app-20261112121917-0000` |
| Executor 0 completed tasks | 29 |
| Executor 1 completed tasks | 31 |
| Failed Spark tasks | 0 |
| HDFS raw-data fsck | HEALTHY; replicas at both DataNode ports |
| Independent pandas comparison | Every latest video's views, likes, and comments matched original CSV |
| Analytical tests | 4 passed |
| Streamlit interaction test | 1 passed (initial load, category filter, sort, empty search, reset) |
| Stop and restart | All seven services stopped and restarted; saved HDFS data remained healthy |

The measured Spark application time was 16.86 seconds in this run. This excludes installation, download, HDFS upload, and dashboard startup. It is not a distributed speedup benchmark.

The source-controlled summary records a completed run. Full generated evidence stays locally in `artifacts/`, including `run.json`, `execution-evidence.json`, `spark-events.jsonl`, `hdfs-report.txt`, `hdfs-blocks.txt`, and `verification.json`.

## Limits

- One physical host; three-PC compliance is not verified.
- Both HDFS replicas share the Mac's physical disk.
- No production security, live YouTube API, or distributed performance claim.
- Fresh installation on another computer and physical LAN networking remain untested.
- Historical trending records are a selected sample, not all videos on YouTube.
