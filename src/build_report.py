"""Builds report/data_ingestion_report_ac12273.pdf from the Spark outputs in output/."""
import json, csv, glob, os, sys
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image,
                                PageBreak, Preformatted, KeepTogether)

OUT = "output"; REP = "report"; os.makedirs(REP, exist_ok=True)
S = json.load(open(f"{OUT}/profile/summary.json"))
LBP = f"{OUT}/lb_summary.json"
LB = json.load(open(LBP)) if os.path.exists(LBP) else None
def rd(path):
    f = glob.glob(path + "/*.csv")[0]
    return list(csv.DictReader(open(f)))
def n(x): return f"{int(x):,}"

st = getSampleStyleSheet()
B = ParagraphStyle("b", parent=st["BodyText"], fontName="Helvetica", fontSize=9.5, leading=13, spaceAfter=5)
H1 = ParagraphStyle("h1", parent=st["Heading1"], fontSize=14, textColor=colors.HexColor("#1F3A5F"), spaceBefore=10, spaceAfter=4)
H2 = ParagraphStyle("h2", parent=st["Heading2"], fontSize=11, textColor=colors.HexColor("#1F3A5F"), spaceBefore=6, spaceAfter=3)
CODE = ParagraphStyle("c", fontName="Courier", fontSize=6.6, leading=8)
SM = ParagraphStyle("sm", parent=B, fontSize=8, leading=10)

def tbl(data, widths, fs=8):
    t = Table([[Paragraph(str(c), ParagraphStyle("t", parent=SM, fontSize=fs, leading=fs + 2)) for c in r] for r in data],
              colWidths=widths, repeatRows=1)
    t.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#DCE6F2")),
                           ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#B0B7C0")),
                           ("VALIGN", (0, 0), (-1, -1), "TOP"),
                           ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4)]))
    return t

def snippet(path, ncols_keep=None, rows=3, width=100):
    lines = open(path).read().splitlines()[: rows + 1]
    return Preformatted("\n".join(l[:width * 2] if False else l for l in lines), CODE)

story = []
story += [Paragraph("Data Ingestion, Profiling and Cleaning", ParagraphStyle("T", parent=st["Title"], fontSize=20, textColor=colors.HexColor("#1F3A5F"))),
          Paragraph("CSCI-GA.2437 Big Data Application Development, Fall 2026. Individual submission.", B),
          Paragraph("<b>Student:</b> Anusha C (NetID ac12273) &nbsp;&nbsp; <b>Teammate:</b> Kal &nbsp;&nbsp; <b>Project:</b> predicting which songs break out (Spotify audio features, Genius lyrics, ListenBrainz listens, MusicBrainz).", B),
          Paragraph("<b>My data sources:</b> Spotify Tracks and ListenBrainz listens. Kal submits the Genius lyrics and MusicBrainz parts separately.", B)]

story += [Paragraph("1. Summary", H1)]
sc = S["cleaning_steps"]
story += [Paragraph(
    f"I ingested the Spotify Tracks dataset ({n(S['raw_rows'])} rows, 20 columns) into Spark, profiled it, and cleaned it down to "
    f"{n(sc['after_dedup_artist_title'])} unique tracks stored as Parquet. All work is PySpark (DataFrames and Spark SQL functions), run in local mode with Spark 3.5.3. "
    "The profile found two things that matter for the rest of the project: the same song appears in several genres (so rows are not unique tracks), and the dataset was sampled evenly, with exactly 1,000 rows for each of 114 genres.", B)]
if LB:
    story += [Paragraph(f"For ListenBrainz I ingested {n(LB['raw_rows'])} real listens (the first 4 million of the 15.5 million in one daily dump), profiled them, and kept {n(LB['cleaning_steps']['after_dedup'])} clean listens. Findings that shape the project: only {LB['pct_rows_with_recording_mbid']}% of listens carry a MusicBrainz recording ID, only {LB['spotify_match']['listens_matched_exact_pct']}% of listens match a Spotify track by artist and title, and the listens date back to 2005 because the dump includes imported history.", B)]
else:
    story += [Paragraph("<b>ListenBrainz:</b> the code is written and tested on a small synthetic sample, but the real dump has not been run yet. See section 4.", B)]

story += [Paragraph("2. Data sources and environment", H1),
          tbl([["Source", "Access", "Size", "Update pattern", "Where stored"],
               ["Spotify Tracks (Kaggle: maharshipandya/-spotify-tracks-dataset)", "Public, free download; community dataset", "20.1 MB CSV, 114,000 rows", "Static", "Raw CSV, then Parquet (output/spotify_raw.parquet)"],
               ["ListenBrainz listens (data.metabrainz.org)", "Public, free; run by MetaBrainz", "Full dump 233 GB. The daily dump I used: 562 MB compressed, 6.9 GB extracted, 15,530,895 listens", "Periodic: daily incrementals, full export about twice a month", "Extracted JSON lines, then Parquet partitioned by month"]],
              [1.7 * inch, 1.3 * inch, 1.2 * inch, 1.2 * inch, 1.6 * inch]),
          Spacer(1, 4),
          Paragraph("<b>Environment:</b> Spark 3.5.3 (PySpark), Java 11, local mode on one 4-core, 3 GB machine, Parquet for storage. The code has no machine-specific paths, so the same scripts can run with spark-submit on a cluster. Spark is used for every step; pandas is used only to write the small samples shown in this report. Because of the memory and time limits of this machine, ListenBrainz was processed as the first 4 million of the dump's 15.5 million listens; the same script (with --resume to split a long run) handles the whole file, or can run on a cluster.", B)]

# ---- Spotify
story += [Paragraph("3. Spotify Tracks", H1), Paragraph("3.1 Ingestion", H2),
          Paragraph("The CSV is read with an explicit schema (no inference), the unnamed row-index column is dropped, and the result is written unchanged to Parquet as the raw zone. Row count after ingestion: "
                    f"<b>{n(S['raw_rows'])}</b>, matching the CSV.", B),
          Paragraph("Raw snippet (first 3 rows, shortened):", SM)]
rs = list(csv.DictReader(open(f"{OUT}/profile/raw_sample.csv")))[:3]
story += [tbl([["track_id", "artists", "track_name", "popularity", "danceability", "tempo", "track_genre"]] +
              [[r["track_id"][:10] + "...", r["artists"], r["track_name"], r["popularity"], r["danceability"], r["tempo"], r["track_genre"]] for r in rs],
              [1.0 * inch, 1.2 * inch, 1.3 * inch, 0.8 * inch, 0.9 * inch, 0.7 * inch, 1.0 * inch])]

story += [Paragraph("3.2 Profiling results", H2)]
miss = S["missing"]
story += [Paragraph(f"<b>Missing values.</b> Only 1 of {n(S['raw_rows'])} rows has gaps: it is missing artists, album_name and track_name together. Every other column is complete.", B),
          Paragraph(f"<b>Uniqueness.</b> {n(S['distinct_track_id'])} distinct track_ids across {n(S['raw_rows'])} rows. {n(S['track_ids_in_more_than_one_row'])} track_ids appear in more than one row because the same song is listed under several genres; "
                    f"{n(S['exact_duplicate_rows'])} rows are exact duplicates of another row. {n(S['track_ids_with_different_popularity_across_rows'])} track_ids have different popularity values in different rows, so deduplication has to pick one on purpose.", B),
          Paragraph(f"<b>Categorical.</b> {S['distinct_genres']} genres with exactly 1,000 rows each (a balanced sample, not the natural genre mix). {n(S['distinct_artist_strings'])} distinct artist strings, and {n(S['distinct_individual_artists'])} individual artists once the semicolon-separated lists are split ({n(S['multi_artist_rows'])} rows list more than one artist). Explicit tracks: {n(S['explicit_counts'].get('True',0))} of {n(S['raw_rows'])}.", B),
          Paragraph(f"<b>Invalid values.</b> popularity is always between 0 and 100 and every 0-1 audio feature stays inside its range. Zeros that cannot be real: tempo = 0 in {S['invalid']['tempo_zero']} rows, time_signature = 0 in {S['invalid']['time_signature_zero']} rows, duration_ms = 0 in {S['invalid']['duration_ms_zero']} row. "
                    f"popularity = 0 occurs in {n(S['invalid']['popularity_zero'])} rows ({S['invalid']['popularity_zero']*100/S['raw_rows']:.1f}%), which is real data (unpopular tracks) but will matter for modeling.", B)]
ns = rd(f"{OUT}/profile/numeric_stats")
story += [Paragraph("Numeric statistics (computed with Spark aggregates and approxQuantile):", SM),
          tbl([["column", "min", "p25", "median", "p75", "max", "mean", "stddev"]] +
              [[r["column"], r["min"], r["p25"], r["median"], r["p75"], r["max"], r["mean"], r["stddev"]] for r in ns],
              [1.3 * inch, 0.8 * inch, 0.8 * inch, 0.8 * inch, 0.8 * inch, 0.9 * inch, 0.9 * inch, 0.9 * inch], fs=7.5)]
ta = rd(f"{OUT}/profile/top_artists")[:10]
story += [Spacer(1, 4), Paragraph("Top 10 individual artists by number of rows: " + ", ".join(f"{r['artist']} ({r['count']})" for r in ta) + ".", SM)]

# funnel chart
steps = [("Raw rows", sc["raw_rows"]), ("Required fields present", sc["after_required_fields"]),
         ("One row per track_id", sc["after_dedup_track_id"]), ("One row per artist + title", sc["after_dedup_artist_title"])]
fig, ax = plt.subplots(figsize=(6.2, 2.0))
ax.barh([s[0] for s in steps][::-1], [s[1] for s in steps][::-1], color="#7FA6D6")
for i, v in enumerate([s[1] for s in steps][::-1]):
    ax.text(v + 1000, i, f"{v:,}", va="center", fontsize=8)
ax.set_xlim(0, 135000); ax.tick_params(labelsize=8); ax.set_xlabel("rows", fontsize=8)
for sp in ["top", "right"]: ax.spines[sp].set_visible(False)
plt.tight_layout(); plt.savefig(f"{REP}/funnel.png", dpi=200); plt.close()

story += [Paragraph("3.3 Cleaning", H2),
          Paragraph("Rules (all in Spark, in src/spotify_job.py): (1) drop rows missing track_id, artists or track_name; (2) normalize artists, track and album names (lowercase, keep letters of any script and digits, strip other punctuation except , &amp; ' - ., collapse spaces), each artist normalized separately in the semicolon list; "
                    "(3) cast explicit to boolean; (4) turn impossible zeros in tempo, time_signature and duration_ms into nulls instead of keeping 0; "
                    "(5) keep one row per track_id (highest popularity) and store all of its genres in an array; (6) keep one row per normalized artist + title (highest popularity), because the same song is often released under several track_ids; "
                    "(7) add artist_list (array) and join_key = artists|track_name for later joins with ListenBrainz and Genius.", B),
          Image(f"{REP}/funnel.png", width=5.6 * inch, height=1.8 * inch),
          Paragraph(f"Result: <b>{n(sc['after_dedup_artist_title'])}</b> clean tracks ({sc['after_dedup_artist_title']*100/S['raw_rows']:.1f}% of raw). Remaining nulls after cleaning: tempo {S['clean_null_counts']['tempo']}, time_signature {S['clean_null_counts']['time_signature']}, duration_ms {S['clean_null_counts']['duration_ms']}. "
                    f"Step 6 removes {sc['after_dedup_track_id']-sc['after_dedup_artist_title']:,} rows that are the same song under a different id; the track-id-level table is kept in the pipeline before this step if we later want one row per release.", B),
          Paragraph("Clean snippet (top 3 tracks by popularity):", SM)]
cs = list(csv.DictReader(open(f"{OUT}/profile/clean_sample.csv")))[:3]
story += [tbl([["artists", "track_name", "popularity", "danceability", "tempo", "genres"]] +
              [[r["artists"], r["track_name"], r["popularity"], r["danceability"], r["tempo"], r["genres"]] for r in cs],
              [1.3 * inch, 1.5 * inch, 0.8 * inch, 0.9 * inch, 0.7 * inch, 1.7 * inch])]

# ---- ListenBrainz
story += [PageBreak(), Paragraph("4. ListenBrainz listens", H1)]
story += [Paragraph("<b>Ingestion.</b> The listens are JSON lines (one listen per line) from the ListenBrainz dump. Spark reads them with an explicit schema containing only the fields we need (artist and track name, release, timestamp, user, and the MusicBrainz IDs in additional_info or mbid_mapping), so it does not scan the data to infer a schema. Malformed lines are kept in a corrupt-record column and counted. The raw data is written to Parquet unchanged.", B)]
if LB:
    mp = LB.get("spotify_match", {})
    story += [Paragraph(f"<b>Source file:</b> {LB.get('source_label','')}", B),
              tbl([["Measure", "Value"],
                   ["Lines read", n(LB["raw_rows"])], ["Corrupt JSON lines", n(LB["corrupt_json_lines"])],
                   ["Time range (UTC)", f"{LB['listened_at_min_utc']} to {LB['listened_at_max_utc']}"],
                   ["Distinct users", n(LB["distinct_users"])], ["Distinct artist names", n(LB["distinct_artist_names"])],
                   ["Distinct artist + title pairs", n(LB["distinct_artist_title_pairs"])],
                   ["Rows with a recording MBID", f"{n(LB['rows_with_recording_mbid'])} ({LB['pct_rows_with_recording_mbid']}%)"],
                   ["Exact duplicate listens", n(LB["exact_duplicate_listens"])], ["Invalid timestamps", n(LB["invalid_timestamps"])]],
                  [2.6 * inch, 3.6 * inch]),
              Paragraph("Cleaning funnel: " + " to ".join(f"{k.replace('_',' ')}: {n(v)}" for k, v in LB["cleaning_steps"].items()) + ".", B)]
    if mp:
        story += [Paragraph("<b>Match against the clean Spotify table.</b> "
                            f"{mp['exact_pairs_matched']:,} of {mp['distinct_artist_title_pairs']:,} distinct artist + title pairs ({mp['exact_pairs_pct']}%) match a Spotify track exactly, covering {mp['listens_matched_exact_pct']}% of listens. "
                            f"Matching on the first artist only raises the pair match to {mp['pairs_matched_primary_artist_pct']}%.", B)]
    import collections
    byyear = collections.Counter()
    for r in rd(f"{OUT}/lb_profile/listens_per_day"):
        byyear[r["day"][:4]] += int(r["count"])
    ys = sorted(byyear)
    fig, ax = plt.subplots(figsize=(6.2, 1.9))
    ax.bar(ys, [byyear[y] for y in ys], color="#7FA6D6")
    ax.set_yscale("log"); ax.tick_params(labelsize=7); ax.set_ylabel("listens (log)", fontsize=8)
    for sp_ in ["top", "right"]: ax.spines[sp_].set_visible(False)
    plt.tight_layout(); plt.savefig(f"{REP}/years.png", dpi=200); plt.close()
    ta2 = rd(f"{OUT}/lb_profile/top_artists")[:8]
    story += [Image(f"{REP}/years.png", width=5.6 * inch, height=1.7 * inch),
              Paragraph("Listens by year (log scale). The listen timestamps span 2005 to 2026 even though this is a dump from Nov 2026, because users import their old listening history.", SM),
              Paragraph("<b>What this means for the project.</b> (1) Only about one listen in five has a recording MBID, so the planned join on MusicBrainz IDs will cover a minority of listens and the normalized artist + title join remains necessary. "
                        "(2) Exact artist + title matching to Spotify is low (about 2% of distinct pairs, 8% of listens), because ListenBrainz covers about 760,000 distinct songs while the Spotify table has 81,000 tracks, and because artist strings differ (featured artists, spelling). "
                        "Matching on the first artist only helps a little, so MusicBrainz IDs or better string matching will be needed before the modeling step. "
                        "(3) The file is not sorted by date (the first million lines already span 2006 to 2026), so the first 4 million lines are not a random sample and may over-represent old imported history, where MBIDs seem rarer (19.8% here versus 2.9% in the first 1 million lines). For the final project we will select listens by date range instead of by file position. "
                        "Most listened artists in this slice: " + ", ".join(f"{r['artist']} ({int(r['count']):,})" for r in ta2) + ".", B)]

else:
    story += [Paragraph("<b>Status:</b> the real ListenBrainz file was not available to the machine that ran this pipeline (the host is blocked from this environment). The code in src/listenbrainz_job.py was tested end to end on a synthetic sample in the same JSON format, and it profiles, cleans, deduplicates and reports the Spotify match rate correctly there. Synthetic results are not reported as findings.", B)]
story += [Paragraph("<b>Cleaning rules:</b> drop listens with no artist or track name or an implausible timestamp (before 2002 or in the future); normalize artist, track and release with the same function used for Spotify (Unicode letters are kept so Japanese, Korean and other non-Latin names survive); take the recording and artist MusicBrainz IDs from mbid_mapping, falling back to additional_info; hash the user name (SHA-256) so the clean table holds no user names; drop duplicates on (user, time, artist, track); add listen date and month and write Parquet partitioned by month.", B)]

story += [Paragraph("5. Scale and what comes next", H1),
          Paragraph("Spotify is small (20 MB). The scale of the project comes from ListenBrainz (233 GB full dump) and the Genius and MusicBrainz sources Kal is handling. For the final project I plan to ingest several months of ListenBrainz listens on the cluster and run the same job there, then do the entity resolution step (MusicBrainz IDs first, normalized artist + title second) with the Spotify join_key. "
                    "Known limits: the Spotify popularity score is a single snapshot with no date, so the models will use ListenBrainz listening counts over time as the outcome; the Spotify sample is balanced across genres, so genre shares in it are not real-world shares.", B),
          Paragraph("6. Files", H1),
          Paragraph("src/spotify_job.py, src/listenbrainz_job.py, src/build_report.py, src/make_synthetic_listens.py (test data generator only), run_all.sh, output/ (Parquet and profiling CSVs), report/.", B)]

doc = SimpleDocTemplate(f"{REP}/data_ingestion_report_ac12273.pdf", pagesize=letter, leftMargin=0.8 * inch, rightMargin=0.8 * inch, topMargin=0.7 * inch, bottomMargin=0.7 * inch,
                        title="Data Ingestion Report - Anusha C (ac12273)")
doc.build(story)
print("built")
