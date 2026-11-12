# Demonstration and explanation

## Before the demonstration

1. Open Terminal in the project directory.
2. Run `.venv/bin/python scripts/cluster.py start`.
3. Run `.venv/bin/python scripts/cluster.py dashboard`.
4. Open dashboard (8501), Hadoop (9870), and Spark master (8080) in your browser.
5. Check `.venv/bin/python scripts/cluster.py status` reports two DataNodes and two Spark workers.
6. Keep `artifacts/` available for persisted evidence after the application exits.

## Five-minute demonstration

**First minute — objective:** “We analyze historical YouTube trending data to compare categories, videos, and engagement. Hadoop stores the data and Spark performs the batch analysis.” State explicitly that this run is a single-host cluster with two worker processes.

**Second minute — storage:** Show two live DataNodes in the HDFS UI. Run `scripts/cluster.py hdfs fsck /youtube/raw -files -blocks -locations` with `.venv/bin/python`. Explain that each block has two replicas in different DataNode directories. Both directories share the Mac's physical disk.

**Third minute — computation:** Start `.venv/bin/python scripts/run_pipeline.py`. Show both workers on the master page; open the active application's UI at port 4040 to show executors and stages. A registered worker alone does not prove that it executed a task. After the job, use saved event-log evidence for tasks per executor.

**Fourth minute — insights:** Show categories by observed views and median engagement, change the date/category filters, search for a channel, and export the ranked table. Explain that daily activity counts trending appearances, while view totals use each video's latest snapshot.

**Fifth minute — validation:** Show `artifacts/verification.json` after running `.venv/bin/python scripts/verify_results.py`. Show the GitHub repository, architecture, and source files. Explain limitations rather than claiming live YouTube coverage or physical-machine fault tolerance.

## Explain the code in plain language

`cluster.py` is the power switch. It writes separate Hadoop configurations and starts the processes with different ports and storage directories.

`download_data.py` gets the original CSV and category mapping. `run_pipeline.py` copies these files into HDFS and starts the Spark application on the standalone master.

`analyze.py` gives each column a type, parses dates, rejects unusable records, removes repeated daily snapshots, attaches category names, and calculates engagement. A Spark window chooses the latest row per video. Grouping produces daily and category summaries. It saves JSON and Parquet back to HDFS.

`app.py` reads the exported results. Date, category, and search filters apply to the cleaned snapshots. It chooses the latest row within the selected dates before computing displayed totals. This keeps the dashboard responsive without starting a Spark job on every click.

## Common viva questions

**Why Hadoop and Spark together?** HDFS provides distributed file storage; Spark provides distributed computation. They solve different parts of the pipeline.

**Are you using MapReduce or YARN?** No. We use Hadoop HDFS with Spark's standalone cluster manager. Spark DataFrames handle the transformations.

**Are these three real machines?** No. The current verified demonstration has one physical Mac with separate master and worker processes. A physical deployment requires reachable node addresses, port configuration, consistent runtimes, and an actual test on three PCs.

**Why use Spark for a dataset that fits on one laptop?** To demonstrate the distributed processing architecture. This dataset does not establish a performance advantage; overhead may make Spark slower at this size.

**What happens if a worker stops?** Spark can reschedule failed work when remaining resources and inputs are available. HDFS can read the surviving replica, but with only one live DataNode it cannot restore replication factor 2. A process failure is different from the Mac failing. Only describe an actual failure demonstration as verified if you perform and record it.

**Why not add views across every row?** Counts are cumulative. A video with 100 views on one day and 150 the next has 150 observed views in its latest snapshot, not 250.

**Is engagement unique viewers?** No. It is the number of likes plus comments relative to views. One person can comment multiple times. It is an analytical convention, not YouTube's official unique-user engagement metric.

**Is India the source of every view?** No. India identifies the trending list; the video view counters are global.

**Why JSON and Parquet?** JSON is easy to inspect and export. Parquet is typed columnar storage useful for downstream analytical reads.

**Why two replicas?** There are two DataNodes. Replication to both demonstrates HDFS block placement, but on one physical disk it does not provide machine-level resilience.

## Suggested three-person responsibilities

- Member 1: pipeline, data definitions, and correctness tests.
- Member 2: HDFS/Spark configuration and cluster demonstration.
- Member 3: dashboard, documentation, and presentation.

Record actual contributions; all members should understand the full data flow.
