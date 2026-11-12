"""Independently verify persisted Spark outputs against the original CSV using pandas."""
import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
run = json.loads((ROOT / 'artifacts/run.json').read_text())
raw = pd.read_csv(ROOT / f"data/raw/{run['country']}videos.csv")
raw['trending_date'] = pd.to_datetime(raw.trending_date, format='%y.%d.%m', errors='coerce')
valid = raw.video_id.str.fullmatch(r'[A-Za-z0-9_-]{11}', na=False) & raw.trending_date.notna()
for col in ['views', 'likes', 'comment_count']:
    raw[col] = pd.to_numeric(raw[col], errors='coerce')
    valid &= raw[col].notna() & (raw[col] >= 0)
expected = raw[valid].sort_values(['views', 'likes', 'comment_count', 'title'], ascending=[False, False, False, True]).drop_duplicates(['video_id', 'trending_date'])
expected_latest = expected.sort_values(['trending_date', 'views']).drop_duplicates('video_id', keep='last').set_index('video_id')
actual = pd.read_json(ROOT / 'artifacts/latest.jsonl', lines=True).set_index('video_id')
assert len(raw) == run['input_rows']
assert len(expected) == run['snapshot_rows'] == run['hdfs_roundtrip_rows']
assert len(expected_latest) == len(actual) == run['unique_videos']
for column in ['views', 'likes', 'comment_count']:
    pd.testing.assert_series_equal(expected_latest[column].sort_index(), actual[column].sort_index(), check_dtype=False, check_names=False)
assert set(actual.index) == set(expected_latest.index)
evidence = {'passed': True, 'checks': ['Raw input row count', 'Valid daily snapshot count', 'HDFS Parquet round trip',
            'Unique-video count and IDs', 'Every latest video view count', 'Every latest like count', 'Every latest comment count'],
            'snapshot_rows': len(expected), 'unique_videos': len(actual)}
(ROOT / 'artifacts/verification.json').write_text(json.dumps(evidence, indent=2))
print(json.dumps(evidence, indent=2))
