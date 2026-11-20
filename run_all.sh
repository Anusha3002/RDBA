#!/bin/bash
# Run from this folder. Needs Java 11+ and: pip install pyspark==3.5.3 pandas pyarrow zstandard
# Usage: ./run_all.sh [listenbrainz .tar.zst or .tar.xz dump]
set -e
export SPARK_LOCAL_IP=127.0.0.1
python3 src/spotify_job.py data/spotify_tracks_raw.csv output
if [ -n "$1" ]; then
  python3 src/extract_dump.py "$1" data/listenbrainz_extracted
  python3 src/listenbrainz_job.py "data/listenbrainz_extracted/*/listens/*/*.listens" output output/spotify_clean.parquet
fi
