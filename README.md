# YouTube Pulse — YouTube Video Statistics

YouTube Pulse is a batch analytics system for historical YouTube trending data. It uses Hadoop Distributed File System (HDFS) for storage, Apache Spark for data processing, and Streamlit for visualization.

The system analyzes views, likes, comments, and engagement across videos, categories, and observation dates. Its processing pipeline covers data acquisition, ingestion, validation, deduplication, aggregation, and presentation. Processed datasets and execution records are retained to support reproducibility and evaluation.

Deployment: one physical Mac, one logical master, and two logical workers.

The [project report](PROJECT_REPORT.md) presents the methodology, architecture, analytical results, and validation for evaluation.

## 1. Introduction and objectives

Trending-video datasets contain repeated observations of videos across multiple dates. View, like, and comment counters are cumulative, while category labels require a separate lookup. Analysis must therefore distinguish historical observations from unique videos to avoid duplicate counting and incorrect aggregation.

The project has the following objectives:

- Store the source records in HDFS with replicated file blocks.
- Use Spark to validate, clean, deduplicate, and analyze the records.
- Compare video categories by observed views and engagement.
- Examine how the composition and engagement of the trending sample change over time.
- Provide video-level exploration and export of filtered results.
- Record task execution on two Spark workers and validate analytical outputs independently.

The analysis compares categories by observed views and median engagement, ranks videos by interaction counts, and measures the number of distinct videos in each daily trending sample.

Processing is performed on demand using a downloaded historical dataset. Live collection through the YouTube API is outside the current scope.

## 2. Dataset and input schema

The source is the public [Trending YouTube Video Statistics dataset by Mitchell J](https://www.kaggle.com/datasets/datasnaek/youtube-new). The default demonstration uses the India files:

| Input file | Contents | Purpose |
|---|---|---|
| `INvideos.csv` | Daily observations of videos appearing on India's trending list | Main input for analysis |
| `IN_category_id.json` | Category IDs and their names | Converts numeric categories into readable labels |

One CSV row represents one video observed on one trending date. It does not necessarily represent a new video or the views gained that day. The same video can have several observations.

The verified input contains 37,352 rows. The retained observations cover 14 November 2017 to 14 June 2018.

The analysis uses the following source fields:

| Field | Meaning | How the project uses it |
|---|---|---|
| `video_id` | Video identifier | Validates and identifies videos across observations |
| `trending_date` | Date the video appeared in the trending sample | Daily grouping and dashboard date filters |
| `title`, `channel_title` | Video and channel names | Search and readable rankings |
| `category_id` | Numeric category identifier | Joined with the category JSON |
| `publish_time` | Video publication timestamp | Calculates the video's age at an observation |
| `views` | Cumulative video views at that observation | View rankings and summaries |
| `likes` | Observed cumulative likes | Like rankings and engagement |
| `comment_count` | Observed cumulative comments | Comment rankings and engagement |
| `comments_disabled`, `ratings_disabled` | Interaction availability flags | Interprets engagement and identifies disabled ratings |
| `video_error_or_removed` | Availability flag in the source | Retained in the cleaned output |

Other source fields, such as descriptions, tags, thumbnails, and dislikes, are not used in the current analytics.

The country identifies which trending list was sampled. The view counters belong to the video globally; they do not measure views originating only in India. Dataset ownership and licensing remain with the source. The original data is downloaded separately rather than committed to this repository.

## 3. System architecture and technology roles

| Technology | Role in this project |
|---|---|
| Hadoop HDFS | Stores the raw inputs and processed outputs as replicated file blocks |
| Apache Spark / PySpark | Processes records in partitions, performs joins and grouping, and produces analytical datasets |
| Spark standalone manager | Allocates executor resources on the two workers |
| Python | Implements orchestration scripts, analysis, validation, and the interface |
| Streamlit and Plotly | Present filters, metrics, tables, and interactive charts |
| pandas | Summarizes exported Spark results for UI filters and independently validates results |

Hadoop and Spark have different responsibilities. HDFS provides storage; Spark provides computation. The project uses the HDFS component of Hadoop with Spark's standalone cluster manager. It does not use Hadoop MapReduce or YARN.

Within HDFS, the NameNode manages metadata: file names, block information, and DataNode locations. The DataNodes hold the actual file blocks. A client consults the NameNode to locate blocks and transfers file data to or from the DataNodes.

Within Spark, the driver runs the application and coordinates its execution. The Spark master allocates resources, and each worker launches an executor. Executors carry out the tasks that process partitions of data.

## 4. Data processing pipeline

```text
Public CSV + category JSON
            |
            v
      Raw files in HDFS
            |
            v
 Spark reads and partitions the data
            |
            v
 Validate -> Deduplicate -> Join categories -> Calculate metrics
            |
            v
 Cleaned snapshots + Latest videos + Daily/category summaries
            |
            v
    JSON and Parquet saved in HDFS
            |
            v
      Local export of JSON results
            |
            v
      Streamlit analytics dashboard
```

### 4.1 Data acquisition and source tracking

`scripts/download_data.py` downloads the public archive and extracts the selected country's CSV and category JSON into `data/raw/`. It also records the source URL, file sizes, and SHA-256 checksums so the input can be identified later.

India is the default. US, GB, CA, DE, and FR are also supported individually. Changing the selected country replaces the dashboard's active export with that country's results.

### 4.2 HDFS ingestion and replication

`scripts/run_pipeline.py` uploads the files to `/youtube/raw/` in HDFS. The local cluster uses two DataNodes and a replication factor of 2, so file blocks are stored on both nodes.

Replication demonstrates HDFS storage behavior. In the current single-Mac setup, both copies share the same physical computer and disk, so this does not provide protection from physical host failure.

### 4.3 Parsing, partitioning, and validation

`pipeline/analyze.py` runs through `spark-submit` against the standalone Spark master. It reads the input directly from HDFS.

The CSV parser supports quoted text and multiline fields. Because this multiline input file initially reads as one partition, the application explicitly redistributes the records into four partitions for downstream processing across the two executors.

The cleaning stage:

- Parses trending dates from `yy.dd.MM`; for example, `17.14.11` becomes `2017-11-14`.
- Converts views, likes, comments, and category IDs into numeric values.
- Parses publication timestamps with the Spark session using UTC.
- Converts the source flags into Boolean values.
- Rejects invalid video IDs, invalid trending dates, and missing or negative view/like/comment counters.

The country label is added from the selected input file. Unmatched category IDs are later labeled `Unknown` rather than silently dropping the video.

### 4.4 Deduplication and latest-observation selection

A Spark window groups rows by `(country, video_id, trending_date)` and retains one observation per group. When duplicates disagree, the ordering prefers higher views, then higher likes, then higher comments, and finally title in ascending order.

Observations on different dates are retained because they are needed for trend analysis. A second window produces a separate latest-observation dataset, containing one most recent row per video and country.

The following illustrative example shows why cumulative counters must not be summed across observation dates:

| Video | Observation date | Cumulative views |
|---|---|---:|
| Video A | 14 November | 1,000 |
| Video A | 15 November | 1,500 |

Both observations are retained in the historical dataset. The latest-observation dataset records 1,500 views for Video A; summing the two observations would incorrectly report 2,500 views. The dashboard selects the latest eligible observation within the specified date range.

### 4.5 Category enrichment and metric calculation

Spark joins category names onto the cleaned records. The small category lookup is broadcast to the executors so they can attach labels during processing.

The application calculates engagement for each observation:

```text
Engagement (%) = 100 × (likes + comment_count) / views
```

For an illustrative video with 10,000 views, 500 likes, and 100 comments, engagement is 6%. A zero-view record or a record with disabled ratings receives a missing engagement value rather than an invalid ratio.

This is an interaction-to-view ratio, not a unique-viewer percentage. Comments-disabled videos remain in the analysis and may have lower measured engagement. Multiple comments from one person are possible, so the ratio is not guaranteed to stay below 100%.

The pipeline then creates:

- Daily summaries: number of trending videos, observed views, and median engagement by country, category, and date.
- Category summaries: number of videos, observed views, observed likes, median views, and median engagement, using each video's latest observation.
- Video-level results: cleaned historical observations and the latest observation per video.

It also derives `days_to_trend` as the difference between the observation date and publication date. This is the video's age at that observation, not necessarily the time it first took to enter a trending list.

### 4.6 Output persistence and verification

Spark writes the processed datasets back to HDFS as JSON and Parquet. JSON supports inspection and dashboard export; Parquet preserves typed data in a columnar format for subsequent analytical reads.

The job reads the saved Parquet data back and checks its row count. The orchestration script then exports the HDFS JSON results into local `artifacts/` files and records the cluster and block-location reports.

### 4.7 Dashboard integration

The Streamlit interface reads the exported Spark-cleaned snapshots. It applies date, category, text-search, and minimum-view filters, then summarizes the selection with pandas.

Dashboard filters operate on the saved results without submitting additional Spark jobs. New source data requires a new batch run before it becomes available in the interface.

## 5. Output datasets and execution artifacts

For the default India run, the processed datasets live under `hdfs://127.0.0.1:9000/youtube/results/IN/`.

| Output directory | One record represents | Main contents |
|---|---|---|
| `cleaned_parquet/` | A valid, deduplicated video observation | Typed snapshot data in Parquet format |
| `snapshots/` | One video on one observed date | Video details, category, counters, flags, engagement, and observation age |
| `latest/` | One video at its latest observed date | Latest counters and engagement per video |
| `daily/` | One country/category/date combination | Trending-video count, observed views, and median engagement |
| `categories/` | One country/category combination | Video count, observed views/likes, median views, and median engagement |

These are Spark output directories containing partition files, not single CSV files. The four JSON datasets are exported locally as `snapshots.jsonl`, `latest.jsonl`, `daily.jsonl`, and `categories.jsonl` in `artifacts/`; each line is one JSON record.

The pipeline also produces the following execution and validation artifacts:

| Local artifact | Recorded information |
|---|---|
| `run.json` | Application ID, input/output counts, dates, duration, and processing definitions |
| `source.json` | Source URL and checksums of the downloaded input files |
| `hdfs-report.txt`, `hdfs-blocks.txt` | DataNode status and locations of replicated input blocks |
| `execution-evidence.json` | Completed tasks per executor, derived from the Spark event log |
| `verification.json` | Independent comparison of processed results with the original CSV |

Full generated data and runtime logs are kept locally and excluded from Git. A compact [verification summary](docs/VERIFICATION.md) and [executor evidence](docs/execution-evidence.json) are included in the repository.

## 6. Dashboard functionality and analytical interpretation

| View | Functionality |
|---|---|
| Overview metrics | Inspect unique videos, latest observed views/likes, and median engagement |
| Category insights | Compare categories by observed views and median engagement |
| Trending activity | Examine the daily count of distinct trending videos and median engagement |
| Video explorer | Search titles/channels, rank by views, likes, comments, or engagement, and export CSV |
| Cluster & methodology | Check live node counts, inspect saved execution evidence, and read processing definitions |

The date filter selects observation dates. View totals are cumulative counters taken from the latest eligible observation of each video; they are not the views gained during the selected period.

Daily charts describe that day's sampled trending videos. Their changes can reflect different videos entering or leaving the sample. They should not be interpreted as overall YouTube traffic or evidence that a category causes higher engagement.

Category charts use median engagement to describe the middle video's ratio rather than letting a small number of extreme ratios dominate an average. Spark's saved aggregate tables use approximate medians; the dashboard calculates medians directly from its filtered export.

## 7. Cluster configuration and resource allocation

The verified demonstration runs directly on an Apple-silicon Mac with 8 GB RAM, without Docker or virtual machines:

| Logical role | Services |
|---|---|
| Master | HDFS NameNode, Spark master, application driver, and dashboard |
| Worker 1 | HDFS DataNode 1 and Spark worker 1 |
| Worker 2 | HDFS DataNode 2 and Spark worker 2 |

The workers have separate storage directories, ports, logs, and process IDs. Each Spark worker supplies one executor core. The services communicate over loopback network interfaces on the same host.

### Resource allocation

Each worker advertises one core and a 768 MiB executor-memory budget to the Spark master. The application requests one core and a 512 MiB Java heap per executor, with a maximum of two executor cores across the application. With both workers available, the master allocates one executor to each worker.

| Resource | Configuration |
|---|---|
| Worker capacity | 1 core and 768 MiB per worker |
| Executor allocation | 1 core and 512 MiB Java heap per executor |
| Application core limit | 2 executor cores |
| Driver Java heap | 768 MiB, in client deploy mode |
| Explicit data repartitioning | 4 partitions |
| SQL shuffle partitions | 4 |

The master allocates executor resources; the application driver schedules individual tasks on those executors. With the default one CPU per task, the two executor cores allow up to two tasks to execute concurrently. Pending tasks are assigned as executor slots become available. Executors terminate when the application finishes, releasing their allocations while the master and workers remain available for subsequent applications.

The configured core count governs Spark task concurrency rather than reserving exclusive physical CPU cores. Java heap settings also exclude Python processes and other native memory overhead. Total host memory consumption includes Hadoop services, the driver, executors, the dashboard, and the operating system.

The current deployment uses one physical host with two logical workers. The [multi-machine guide](docs/MULTI_MACHINE.md) explains the address, port, runtime, and deployment changes required to distribute these services across separate computers.

The dataset is small enough to process on a single computer. The implementation demonstrates distributed storage and execution mechanisms; it does not establish a performance advantage over single-process analysis at this scale.

## 8. Results and validation

The following results were recorded from the completed India run on 12 November 2026:

| Measure | Verified result |
|---|---:|
| Original input rows | 37,352 |
| Invalid or duplicate daily rows removed | 5,087 |
| Retained daily video observations | 32,265 |
| Distinct videos | 16,305 |
| Live HDFS DataNodes | 2 |
| Spark executors that completed tasks | 2 |
| Completed tasks on executor 0 / executor 1 | 29 / 31 |
| Failed Spark tasks | 0 |
| Automated tests passed | 5 |

Validation covers analytical correctness, storage integrity, executor activity, interface behavior, and service lifecycle:

1. Analytical tests check daily deduplication, latest-observation selection, invalid records, zero views, disabled ratings, and country isolation.
2. Independent pandas verification compares every latest video's views, likes, and comments against the original CSV, as well as row counts and video IDs.
3. HDFS verification checks saved Parquet row counts and reports healthy replicated input blocks.
4. Spark event-log inspection proves that both executors completed tasks; registered workers alone would not establish this.
5. Dashboard testing exercises initial loading, a category filter, ranking changes, an empty search, and reset behavior.
6. Lifecycle verification confirms that the cluster can stop and restart while preserving stored HDFS data.

These are results of the recorded local run. Fresh installation on other computers and execution across a physical LAN remain unverified.

## 9. Installation and execution

### 9.1 Existing installation

Open Terminal in the repository and run:

```bash
bash run.sh
```

This starts the cluster and dashboard, reusing saved results. Open [YouTube Pulse](http://127.0.0.1:8501).

To rerun ingestion, Spark analysis, independent validation, and executor evidence collection:

```bash
bash run.sh --refresh
```

To stop the services and free memory while keeping the data:

```bash
.venv/bin/python scripts/cluster.py stop
```

Run one pipeline at a time.

### 9.2 Initial installation on macOS

Prerequisites are Java 11, Python 3.11, Git, curl, and tar. Install binaries suitable for the machine's CPU architecture. Then:

```bash
git clone https://github.com/Avighn0611/BDA-youtube.git
cd BDA-youtube
bash scripts/setup.sh
bash run.sh --refresh
```

The setup script installs the pinned Python dependencies from `requirements.lock.txt`, downloads Hadoop 3.3.6, and verifies its Apache SHA-512 checksum. The project uses Spark/PySpark 3.5.7.

Runtimes live in `~/.cache/youtube-pulse`; `.runtime` and `.venv` are local symlinks. No global Python packages or SSH server are required. If the Hadoop archive download is slow, the same archive can be obtained from a mirror; it must still pass the official checksum check.

### 9.3 Monitoring and verification

| Interface | URL |
|---|---|
| Application dashboard | http://127.0.0.1:8501 |
| HDFS NameNode | http://127.0.0.1:9870 |
| Spark master | http://127.0.0.1:8080 |
| Spark workers | http://127.0.0.1:8081 and http://127.0.0.1:8082 |
| Spark application | http://127.0.0.1:4040 while a job is running |

```bash
# Inspect nodes and block replication
.venv/bin/python scripts/cluster.py status
.venv/bin/python scripts/cluster.py hdfs dfsadmin -report
.venv/bin/python scripts/cluster.py hdfs fsck /youtube/raw -files -blocks -locations

# Inspect processed files
.venv/bin/python scripts/cluster.py hdfs dfs -ls /youtube/results/IN

# Run tests and validate the existing outputs
.venv/bin/python -m pytest tests -q
.venv/bin/python scripts/verify_results.py
.venv/bin/python scripts/collect_evidence.py
```

`artifacts/pipeline.log` contains the analysis log; `.runtime/state/logs/` contains service logs. Spark event logs are retained in HDFS under `/youtube/events`. Startup preserves an existing filesystem rather than formatting it again. Processing time depends on available memory and system load.

## 10. Repository structure and supporting documentation

| File | Responsibility |
|---|---|
| [`run.sh`](run.sh) | Starts the demonstration and optionally refreshes the results |
| [`scripts/setup.sh`](scripts/setup.sh) | Installs the isolated environment and Hadoop runtime |
| [`scripts/cluster.py`](scripts/cluster.py) | Configures and controls the NameNode, DataNodes, Spark services, and dashboard |
| [`scripts/download_data.py`](scripts/download_data.py) | Downloads the dataset and records its source and checksums |
| [`scripts/run_pipeline.py`](scripts/run_pipeline.py) | Uploads inputs, submits the Spark application, and exports outputs |
| [`pipeline/analyze.py`](pipeline/analyze.py) | Implements distributed cleaning, enrichment, and aggregation |
| [`dashboard/app.py`](dashboard/app.py) | Implements the interactive interface |
| [`scripts/verify_results.py`](scripts/verify_results.py) | Independently checks the analytical results |
| [`scripts/collect_evidence.py`](scripts/collect_evidence.py) | Extracts execution evidence from Spark event logs |
| [`tests/`](tests/) | Contains analytical and dashboard interaction tests |
| [`docs/DEMO_GUIDE.md`](docs/DEMO_GUIDE.md) | Provides a demonstration walkthrough and viva explanations |
| [`docs/MULTI_MACHINE.md`](docs/MULTI_MACHINE.md) | Describes migration to three physical machines |
| [`docs/VERIFICATION.md`](docs/VERIFICATION.md) | Records the completed local verification |

Repository: [Avighn0611/BDA-youtube](https://github.com/Avighn0611/BDA-youtube).

## References

- [Dataset: Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new)
- [Apache Hadoop: Single Node Setup](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Apache Spark: Standalone Clusters](https://spark.apache.org/docs/3.5.7/spark-standalone.html)
- [Apache Spark: Submitting Applications](https://spark.apache.org/docs/3.5.7/submitting-applications.html)
- [Streamlit Documentation](https://docs.streamlit.io/)
