"""
ListenBrainz listens: ingestion, profiling and cleaning in PySpark.
Usage: python src/listenbrainz_job.py <listens_glob_or_dir> <out_dir> [spotify_clean.parquet]
Input: JSON-lines listen files (field "timestamp" in dumps; "listened_at" also accepted) (the *.listens files inside a ListenBrainz dump, one JSON listen per line).
Writes: <out_dir>/lb_raw.parquet, lb_clean.parquet (partitioned by month), lb_profile/*, lb_summary.json
If a Spotify clean parquet is given, also reports how many listens/tracks match a Spotify track.
"""
import sys, json
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *

src, out = sys.argv[1], sys.argv[2]
spotify = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].startswith("--") else None
spark = (SparkSession.builder.appName("listenbrainz_profile_clean")
         .config("spark.sql.shuffle.partitions", "32").config("spark.driver.memory", "2g").config("spark.local.dir", "/tmp/sparktmp").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Explicit schema (only the fields we use) so Spark does not scan the data to infer it.
mapping = StructType([StructField("recording_mbid", StringType()),
                      StructField("artist_mbids", ArrayType(StringType())),
                      StructField("release_mbid", StringType()),
                      StructField("artists", ArrayType(StructType([StructField("artist_mbid", StringType()),
                                                                    StructField("artist_credit_name", StringType())])))])
addl = StructType([StructField("recording_mbid", StringType()), StructField("track_mbid", StringType()),
                   StructField("artist_mbids", ArrayType(StringType())),
                   StructField("release_mbid", StringType()),
                   StructField("duration_ms", LongType()), StructField("duration", LongType()),
                   StructField("music_service", StringType()),
                   StructField("submission_client", StringType())])
tm = StructType([StructField("artist_name", StringType()), StructField("track_name", StringType()),
                 StructField("release_name", StringType()),
                 StructField("additional_info", addl), StructField("mbid_mapping", mapping)])
schema = StructType([StructField("listened_at", LongType()), StructField("timestamp", LongType()), StructField("inserted_at", StringType()),
                     StructField("user_name", StringType()), StructField("user_id", StringType()),
                     StructField("recording_msid", StringType()), StructField("track_metadata", tm),
                     StructField("_corrupt", StringType())])

# ---------- 1. Ingest ----------
raw = (spark.read.schema(schema).option("mode", "PERMISSIVE")
       .option("columnNameOfCorruptRecord", "_corrupt").json(src))
import os
RESUME = "--resume" in sys.argv      # reuse finished Parquet stages (lets a long run be split into several runs)
def done(path): return RESUME and os.path.exists(path + "/_SUCCESS")
if not done(f"{out}/lb_raw.parquet"):
    raw.write.mode("overwrite").parquet(f"{out}/lb_raw.parquet")   # raw zone, unchanged (+ corrupt-record column)
df = spark.read.parquet(f"{out}/lb_raw.parquet")
n_raw = df.count()
n_corrupt = df.filter(F.col("_corrupt").isNotNull()).count()
summary = {"raw_rows": n_raw, "corrupt_json_lines": n_corrupt}

def save(sdf, name):
    sdf.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/lb_profile/{name}")

flat = (df.select(
    F.coalesce("user_name", "user_id").alias("user"), F.coalesce("listened_at", "timestamp").alias("listened_at"), "recording_msid",
    F.col("track_metadata.artist_name").alias("artist_name"),
    F.col("track_metadata.track_name").alias("track_name"),
    F.col("track_metadata.release_name").alias("release_name"),
    F.coalesce("track_metadata.mbid_mapping.recording_mbid",
               "track_metadata.additional_info.recording_mbid",
               "track_metadata.additional_info.track_mbid").alias("recording_mbid"),
    F.coalesce(F.element_at("track_metadata.mbid_mapping.artist_mbids", 1),
               F.element_at("track_metadata.additional_info.artist_mbids", 1)).alias("artist_mbid"),
    F.coalesce("track_metadata.additional_info.duration_ms",
               (F.col("track_metadata.additional_info.duration") * 1000)).alias("duration_ms")))

# ---------- 2. Profile ----------
def blank(c): return F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
cols = flat.columns
miss = flat.select([F.sum(blank(c).cast("int")).alias(c) for c in cols]).first().asDict()
save(spark.createDataFrame([(c, int(v), round(100.0 * v / max(n_raw, 1), 3)) for c, v in miss.items()],
                           ["column", "missing", "missing_pct"]), "missing")
summary["missing"] = {c: int(v) for c, v in miss.items()}
ts = flat.select(F.min("listened_at").alias("mn"), F.max("listened_at").alias("mx")).first()
summary["listened_at_min"] = ts["mn"]; summary["listened_at_max"] = ts["mx"]
summary["listened_at_min_utc"] = (spark.sql(f"select cast(from_unixtime({ts['mn']}) as string)").first()[0]
                                  if ts["mn"] is not None else None)
summary["listened_at_max_utc"] = (spark.sql(f"select cast(from_unixtime({ts['mx']}) as string)").first()[0]
                                  if ts["mx"] is not None else None)
summary["distinct_users"] = flat.select("user").distinct().count()
summary["distinct_artist_names"] = flat.select(F.lower("artist_name")).distinct().count()
summary["distinct_artist_title_pairs"] = flat.select(F.lower("artist_name"), F.lower("track_name")).distinct().count()
summary["rows_with_recording_mbid"] = flat.filter(~blank("recording_mbid")).count()
summary["rows_with_artist_mbid"] = flat.filter(~blank("artist_mbid")).count()
summary["pct_rows_with_recording_mbid"] = round(100.0 * summary["rows_with_recording_mbid"] / max(n_raw, 1), 2)
summary["invalid_timestamps"] = flat.filter(
    F.col("listened_at").isNull() | (F.col("listened_at") < 1009843200) |   # before 2002-01-01
    (F.col("listened_at") > F.unix_timestamp() + 86400)).count()
summary["exact_duplicate_listens"] = int(n_raw - flat.dropDuplicates(
    ["user", "listened_at", "artist_name", "track_name"]).count())
save(flat.groupBy(F.lower("artist_name").alias("artist")).count().orderBy(F.desc("count")).limit(25), "top_artists")
save(flat.groupBy(F.lower("artist_name").alias("artist"), F.lower("track_name").alias("track"))
     .count().orderBy(F.desc("count")).limit(25), "top_tracks")
save(flat.groupBy(F.to_date(F.from_unixtime("listened_at")).alias("day")).count().orderBy("day"), "listens_per_day")
save(flat.groupBy("user").count().orderBy(F.desc("count")).limit(10)
     .withColumn("user", F.substring(F.sha2("user", 256), 1, 10)), "top_users_hashed")

# ---------- 3. Clean ----------
def norm(c):
    x = F.lower(F.trim(c))
    x = F.regexp_replace(x, r"[^\p{L}\p{N}\s,&'\-\.]", "")   # keep letters (any script), digits, space and , & ' - .
    return F.trim(F.regexp_replace(x, r"\s+", " "))

c = flat.filter(~blank("artist_name") & ~blank("track_name") & F.col("listened_at").isNotNull()
                & (F.col("listened_at") >= 1009843200) & (F.col("listened_at") <= F.unix_timestamp() + 86400))
n_after_required = c.count()
c = (c.withColumn("artist", norm(F.col("artist_name"))).withColumn("track", norm(F.col("track_name")))
     .withColumn("release", norm(F.col("release_name")))
     .withColumn("user_hash", F.sha2(F.col("user"), 256))
     .withColumn("listened_ts", F.to_timestamp(F.from_unixtime("listened_at")))
     .withColumn("listen_date", F.to_date("listened_ts"))
     .withColumn("listen_month", F.date_format("listened_ts", "yyyy-MM"))
     .withColumn("recording_mbid", F.lower(F.trim("recording_mbid")))
     .withColumn("artist_mbid", F.lower(F.trim("artist_mbid")))
     .filter((F.col("artist") != "") & (F.col("track") != "")))
c = c.dropDuplicates(["user_hash", "listened_at", "artist", "track"])
clean = c.select("user_hash", "listened_at", "listened_ts", "listen_date", "listen_month", "artist", "track",
                 "release", "recording_mbid", "artist_mbid", "duration_ms",
                 F.concat_ws("|", "artist", "track").alias("join_key"))
if not done(f"{out}/lb_clean.parquet"):
    clean.write.mode("overwrite").partitionBy("listen_month").parquet(f"{out}/lb_clean.parquet")
clean = spark.read.parquet(f"{out}/lb_clean.parquet")
n_clean = clean.count()
summary["cleaning_steps"] = {"raw_rows": n_raw, "after_required_and_valid_time": n_after_required,
                             "after_dedup": n_clean}

# ---------- 4. Match against Spotify (reports the match rate; no silent loss) ----------
if spotify:
    sp = spark.read.parquet(spotify)
    sp_keys = sp.select("join_key").distinct()
    sp_primary = sp.select(F.concat_ws("|", F.element_at("artist_list", 1), "track_name").alias("pk")).distinct()
    pairs = clean.groupBy("join_key", "artist", "track").count()
    pairs = pairs.withColumn("pk", F.concat_ws("|", F.trim(F.regexp_extract("artist", r"^(.*?)(?:\s+(?:feat\.?|ft\.?|featuring|&|,|x)\s+.*)?$", 1)), "track"))
    exact = pairs.join(sp_keys, "join_key", "left_semi")
    prim = pairs.join(sp_primary, pairs.pk == sp_primary.pk, "left_semi")
    tot_listens = clean.count(); tot_pairs = pairs.count()
    summary["spotify_match"] = {
        "distinct_artist_title_pairs": tot_pairs,
        "exact_pairs_matched": exact.count(),
        "exact_pairs_pct": round(100.0 * exact.count() / max(tot_pairs, 1), 2),
        "listens_matched_exact": int(exact.agg(F.sum("count")).first()[0] or 0),
        "listens_matched_exact_pct": round(100.0 * int(exact.agg(F.sum("count")).first()[0] or 0) / max(tot_listens, 1), 2),
        "pairs_matched_primary_artist": prim.count(),
        "pairs_matched_primary_artist_pct": round(100.0 * prim.count() / max(tot_pairs, 1), 2),
    }

clean.drop("join_key").orderBy("listened_at").limit(5).toPandas().to_csv(f"{out}/lb_profile/clean_sample.csv", index=False)
df.limit(3).select(F.to_json(F.struct("*")).alias("json")).toPandas().to_csv(f"{out}/lb_profile/raw_sample.csv", index=False)
with open(f"{out}/lb_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2, default=str)
print(json.dumps(summary, indent=2, default=str))
spark.stop()
