"""Download original public Kaggle archive; extract only the selected country."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import zipfile

import requests
from cluster import ROOT, RUNTIME

URL = 'https://www.kaggle.com/api/v1/datasets/download/datasnaek/youtube-new'
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--country', choices=['IN', 'US', 'GB', 'CA', 'DE', 'FR'], default='IN')
args = parser.parse_args()
archive = RUNTIME / 'downloads/youtube-new.zip'
archive.parent.mkdir(parents=True, exist_ok=True)
if not archive.exists():
    temp = archive.with_suffix('.download')
    with requests.get(URL, stream=True, timeout=(30, 180)) as response:
        response.raise_for_status()
        with temp.open('wb') as destination:
            for chunk in response.iter_content(1024 * 1024):
                destination.write(chunk)
    if not zipfile.is_zipfile(temp):
        raise SystemExit('Kaggle did not return a ZIP. Download the dataset manually from its page.')
    temp.replace(archive)
raw = ROOT / 'data/raw'
raw.mkdir(parents=True, exist_ok=True)
records = []
with zipfile.ZipFile(archive) as bundle:
    for filename in [f'{args.country}videos.csv', f'{args.country}_category_id.json']:
        member = next((name for name in bundle.namelist() if Path(name).name == filename), None)
        if member is None:
            raise SystemExit(f'{filename} missing from archive')
        payload = bundle.read(member)
        (raw / filename).write_bytes(payload)
        records.append({'file': filename, 'bytes': len(payload), 'sha256': hashlib.sha256(payload).hexdigest()})
        print(f'Extracted {filename}: {len(payload):,} bytes')
provenance = {'source': 'https://www.kaggle.com/datasets/datasnaek/youtube-new',
              'download_url': URL, 'country': args.country, 'files': records,
              'scope': 'Historical trending-list snapshots, not all YouTube videos.'}
(raw / f'{args.country}_source.json').write_text(json.dumps(provenance, indent=2))
