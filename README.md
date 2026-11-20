# Data ingestion, profiling and cleaning - Anusha C (ac12273)
CSCI-GA.2437 Big Data Application Development, Fall 2026. Sources: Spotify Tracks, ListenBrainz listens.

- `src/spotify_job.py` - ingest, profile, clean Spotify (PySpark). Needs `data/spotify_tracks_raw.csv`.
- `src/listenbrainz_job.py` - ingest, profile, clean ListenBrainz listens and report the Spotify match rate.
- `src/extract_dump.py` - unpack a ListenBrainz `.tar.zst` dump.
- `src/build_report.py` - builds `report/data_ingestion_report_ac12273.pdf` from `output/`.
- `src/make_synthetic_listens.py` - makes fake listens for pipeline testing only. Not used for any result.
- `run_all.sh` - runs everything. Python 3 with `pyspark==3.5.3 pandas pyarrow zstandard reportlab matplotlib`, Java 11+.
- `data/sample_real_8_listens/` - 8 real listens used to check the parser. The raw dump is not included (562 MB).
- `output/profile/`, `output/lb_profile/` - profiling CSVs and samples; `output/*.parquet` - raw and clean tables.

ListenBrainz result in the report: first 4,000,000 of the 15,530,895 listens in dump 2798 (2026-11-20).
Long runs can be split with `--resume`.
