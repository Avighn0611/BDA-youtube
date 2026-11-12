#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
YT_RUNTIME="${YT_RUNTIME:-$HOME/.cache/youtube-pulse}"
export YT_RUNTIME
mkdir -p "$YT_RUNTIME/downloads"
if [[ ! -x "$YT_RUNTIME/venv/bin/python" ]]; then
  python3.11 -m venv "$YT_RUNTIME/venv"
fi
"$YT_RUNTIME/venv/bin/python" -m ensurepip --upgrade
"$YT_RUNTIME/venv/bin/python" -m pip install -r requirements.lock.txt
if [[ ! -d "$YT_RUNTIME/hadoop-3.3.6" ]]; then
  archive="$YT_RUNTIME/downloads/hadoop-3.3.6.tar.gz"
  if [[ ! -f "$archive" ]]; then
    curl -fL --retry 3 https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz -o "$archive"
  fi
  curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz.sha512 -o "$archive.sha512"
  "$YT_RUNTIME/venv/bin/python" scripts/verify_download.py "$archive" "$archive.sha512"
  tar -xzf "$archive" -C "$YT_RUNTIME"
fi
[[ -e .venv ]] || ln -s "$YT_RUNTIME/venv" .venv
[[ -e .runtime ]] || ln -s "$YT_RUNTIME" .runtime
echo 'Ready. Run: .venv/bin/python scripts/download_data.py'
