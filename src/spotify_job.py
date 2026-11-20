"""
Spotify Tracks: ingestion, profiling and cleaning in PySpark.
Usage: spark-submit src/spotify_job.py <raw_csv> <out_dir>
Writes: <out_dir>/spotify_raw.parquet, spotify_clean.parquet, profile/*, profile/summary.json
"""
import sys, json
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               DoubleType, LongType)

raw_csv, out = sys.argv[1], sys.argv[2]
spark = (SparkSession.builder.appName("spotify_profile_clean")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("row_index", LongType()), StructField("track_id", StringType()),
    StructField("artists", StringType()), StructField("album_name", StringType()),
    StructField("track_name", StringType()), StructField("popularity", IntegerType()),
    StructField("duration_ms", LongType()), StructField("explicit", StringType()),
    StructField("danceability", DoubleType()), StructField("energy", DoubleType()),
    StructField("key", IntegerType()), StructField("loudness", DoubleType()),
    StructField("mode", IntegerType()), StructField("speechiness", DoubleType()),
    StructField("acousticness", DoubleType()), StructField("instrumentalness", DoubleType()),
    StructField("liveness", DoubleType()), StructField("valence", DoubleType()),
    StructField("tempo", DoubleType()), StructField("time_signature", IntegerType()),
    StructField("track_genre", StringType()),
])

# ---------- 1. Ingest: CSV -> Parquet (raw zone, unchanged) ----------
df = (spark.read.option("header", True).option("multiLine", True)
      .option("escape", '"').schema(schema).csv(raw_csv)).drop("row_index")
df.write.mode("overwrite").parquet(f"{out}/spotify_raw.parquet")
df = spark.read.parquet(f"{out}/spotify_raw.parquet")
n_raw = df.count()
cols = df.columns
num_cols = ["popularity", "duration_ms", "danceability", "energy", "key", "loudness", "mode",
            "speechiness", "acousticness", "instrumentalness", "liveness", "valence",
            "tempo", "time_signature"]
summary = {"raw_rows": n_raw, "columns": len(cols)}

def save(sdf, name):
    sdf.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/profile/{name}")

# ---------- 2. Profile ----------
miss = df.select([F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), 1)
                        .otherwise(0)).alias(c) for c in cols]).first().asDict()
save(spark.createDataFrame([(c, int(v), round(100.0 * v / n_raw, 4)) for c, v in miss.items()],
                           ["column", "missing", "missing_pct"]), "missing")
summary["missing"] = {c: int(v) for c, v in miss.items() if v}

aggs = []
for c in num_cols:
    aggs += [F.min(c).alias(f"{c}__min"), F.max(c).alias(f"{c}__max"),
             F.mean(c).alias(f"{c}__mean"), F.stddev(c).alias(f"{c}__std")]
r = df.agg(*aggs).first().asDict()
q = df.approxQuantile(num_cols, [0.25, 0.5, 0.75], 0.001)
rows = []
for i, c in enumerate(num_cols):
    vals = [r[f"{c}__min"], q[i][0], q[i][1], q[i][2], r[f"{c}__max"], r[f"{c}__mean"], r[f"{c}__std"]]
    rows.append((c,) + tuple(None if v is None else round(float(v), 4) for v in vals))
save(spark.createDataFrame(rows, ["column", "min", "p25", "median", "p75", "max", "mean", "stddev"]),
     "numeric_stats")

summary["distinct_track_id"] = df.select("track_id").distinct().count()
summary["distinct_genres"] = df.select("track_genre").distinct().count()
summary["distinct_artist_strings"] = df.select("artists").distinct().count()
summary["explicit_counts"] = {str(k): v for k, v in
                              df.groupBy("explicit").count().rdd.map(lambda x: (x[0], x[1])).collect()}
save(df.groupBy("track_genre").count().orderBy(F.desc("count")), "genre_counts")
artist_ex = df.withColumn("artist", F.explode(F.split("artists", ";")))
summary["distinct_individual_artists"] = artist_ex.select(F.lower(F.trim("artist"))).distinct().count()
save(artist_ex.groupBy(F.lower(F.trim("artist")).alias("artist")).count()
     .orderBy(F.desc("count")).limit(25), "top_artists")
summary["multi_artist_rows"] = df.filter(F.col("artists").contains(";")).count()

summary["track_ids_in_more_than_one_row"] = df.groupBy("track_id").count().filter("count > 1").count()
summary["rows_beyond_first_per_track_id"] = int(n_raw - summary["distinct_track_id"])
summary["exact_duplicate_rows"] = int(n_raw - df.dropDuplicates().count())
summary["invalid"] = {
    "duration_ms_zero": df.filter("duration_ms = 0").count(),
    "tempo_zero": df.filter("tempo = 0").count(),
    "time_signature_zero": df.filter("time_signature = 0").count(),
    "popularity_zero": df.filter("popularity = 0").count(),
    "popularity_out_of_0_100": df.filter("popularity < 0 OR popularity > 100").count(),
    "audio_feature_out_of_0_1": df.filter(" OR ".join(
        f"{c} < 0 OR {c} > 1" for c in ["danceability", "energy", "speechiness", "acousticness",
                                        "instrumentalness", "liveness", "valence"])).count(),
}
summary["track_ids_with_different_popularity_across_rows"] = (
    df.groupBy("track_id").agg(F.countDistinct("popularity").alias("n")).filter("n > 1").count())

# ---------- 3. Clean ----------
def norm(c):
    x = F.lower(F.trim(c))
    x = F.regexp_replace(x, r"[^\p{L}\p{N}\s,&'\-\.]", "")   # keep letters (any script), digits, space and , & ' - .
    return F.trim(F.regexp_replace(x, r"\s+", " "))

bad = ["", "none", "null", "nan"]
c1 = df
for c in ["track_id", "artists", "track_name"]:
    c1 = c1.filter(F.col(c).isNotNull() & (~F.lower(F.trim(F.col(c))).isin(bad)))
n_after_required = c1.count()
c1 = (c1.withColumn("artists_norm", F.array_join(F.transform(F.split("artists", ";"),
                                                             lambda a: norm(a)), ";"))
      .withColumn("track_name_norm", norm(F.col("track_name")))
      .withColumn("album_name_norm", norm(F.col("album_name")))
      .withColumn("explicit", F.lower(F.trim("explicit")).isin("true", "1", "yes"))
      .withColumn("genre", norm(F.col("track_genre"))))
# physically impossible zeros become null rather than real values
for c in ["tempo", "time_signature", "duration_ms"]:
    c1 = c1.withColumn(c, F.when(F.col(c) > 0, F.col(c)))
# Step A: one row per track_id (same song under several genres); genres kept as an array
w = Window.partitionBy("track_id").orderBy(F.desc("popularity"), F.asc("genre"))
by_id = (c1.withColumn("rn", F.row_number().over(w))
         .withColumn("genres", F.collect_set("genre").over(Window.partitionBy("track_id")))
         .filter("rn = 1").drop("rn", "track_genre", "genre"))
n_after_id = by_id.count()
# Step B: one row per normalized artist+title (re-releases under other ids); keep most popular
w2 = Window.partitionBy("artists_norm", "track_name_norm").orderBy(F.desc("popularity"), F.asc("track_id"))
clean = (by_id.withColumn("rn", F.row_number().over(w2)).filter("rn = 1").drop("rn")
         .withColumn("genres", F.array_sort("genres"))
         .select("track_id", F.col("artists_norm").alias("artists"),
                 F.col("album_name_norm").alias("album_name"),
                 F.col("track_name_norm").alias("track_name"), "popularity", "duration_ms",
                 "explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness",
                 "acousticness", "instrumentalness", "liveness", "valence", "tempo",
                 "time_signature", "genres")
         .withColumn("artist_list", F.split("artists", ";"))
         .withColumn("join_key", F.concat_ws("|", "artists", "track_name")))
clean.write.mode("overwrite").parquet(f"{out}/spotify_clean.parquet")
clean = spark.read.parquet(f"{out}/spotify_clean.parquet")
n_clean = clean.count()
summary["cleaning_steps"] = {"raw_rows": n_raw, "after_required_fields": n_after_required,
                             "after_dedup_track_id": n_after_id, "after_dedup_artist_title": n_clean}
summary["clean_null_counts"] = clean.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in
     ["tempo", "time_signature", "duration_ms"]]).first().asDict()
clean.drop("artist_list", "join_key").orderBy(F.desc("popularity")).limit(5) \
    .toPandas().to_csv(f"{out}/profile/clean_sample.csv", index=False)
df.limit(5).toPandas().to_csv(f"{out}/profile/raw_sample.csv", index=False)
with open(f"{out}/profile/summary.json", "w") as fh:
    json.dump(summary, fh, indent=2, default=str)
print(json.dumps(summary, indent=2, default=str))
spark.stop()
