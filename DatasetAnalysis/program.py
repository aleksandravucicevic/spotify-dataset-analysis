import os, shutil, json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, NumericType

spark = None

# kreiranje globalne SparkSession instance ako ona vec ne postoji
def get_spark_session():
    global spark
    if spark not in globals():
        spark = SparkSession.builder.appName("SpotifyDataAnalysis").getOrCreate()
        
    return spark

# definisanje seme i ucitavanje podataka u skladu s tim
def load_data(spark):
    spark.sparkContext.setLogLevel("ERROR")

    data_schema = StructType([
        StructField("",IntegerType(), True),
        StructField("track_id",StringType(), True),
        StructField("artists",StringType(), True),
        StructField("album_name",StringType(), True),
        StructField("track_name",StringType(), True),
        StructField("popularity",IntegerType(), True),
        StructField("duration_ms",DoubleType(), True),
        StructField("explicit",BooleanType(), True),
        StructField("danceability",DoubleType(), True),
        StructField("energy",DoubleType(), True),
        StructField("key",IntegerType(), True),
        StructField("loudness",DoubleType(), True),
        StructField("mode",IntegerType(), True),
        StructField("speechiness",DoubleType(), True),
        StructField("acousticness",DoubleType(), True),
        StructField("instrumentalness",DoubleType(), True),
        StructField("liveness",DoubleType(), True),
        StructField("valence",DoubleType(), True),
        StructField("tempo",DoubleType(), True),
        StructField("time_signature",IntegerType(), True),
        StructField("track_genre",StringType(), True)
    ])

    df = spark.read.csv("../opos/data/dataset.csv", header=True, quote='"', escape='"', multiLine=True, mode="FAILFAST", schema= data_schema)
    df.cache()
    return df

# ispis rezultata u jednom .json fajlu
def single_json_output(dir_name: str, file_name: str):
    json_dir = os.path.join("../opos/output/", dir_name)
    output_file = os.path.join("../opos/output/", file_name)
    
    try:
        json_files = sorted(f for f in os.listdir(json_dir) if f.startswith("part-") and f.endswith(".json"))
    
        if not json_files:
            print(f"Error: no part-*.json files found in {json_dir}")
            return
        
        all_objects = []
        for file in json_files:
            file_path = os.path.join(json_dir, file)
            with open(file_path, "r", encoding="utf-8") as f:
                all_objects.extend(json.loads(line) for line in f)
                
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_objects, f, indent=4, ensure_ascii=False)
            
        shutil.rmtree(json_dir)
        
    except Exception as e:
        print(f"Error while merging JSON files: {e}")
        

# inicijalna obrada podataka 
def preprocessing(df):
    distribution = {}
    categorical_columns = {"album_name", "track_genre", "artists"}
    # kolone koje nemaju znacaj u statistickom smislu
    irrelevant_columns = {"track_id", ""}
    
    for column in df.columns:
        
        if column in irrelevant_columns:
            continue

        # kolona 'explicit' kao boolean kolona se obradjuje odvojeno
        if column == "explicit":
            explicit_counts = df.groupBy("explicit").count().collect()
            explicit_dict = {row["explicit"]: row["count"] for row in explicit_counts}
            distribution[column] = {
                "true_count": explicit_dict.get(True, 0),
                "false_count": explicit_dict.get(False, 0)
            }
            continue
        
        dtype = [f.dataType for f in df.schema.fields if f.name == column][0]

        try:
            # obrada numerickih kolona
            if isinstance(dtype, NumericType):
                stats = df.select(column).summary("count", "mean", "stddev", "min", "max").collect()
                dist_dict = {
                    "count": stats[0][column],
                    "mean": stats[1][column],
                    "stddev": stats[2][column],
                    "min": stats[3][column],
                    "max": stats[4][column]
                }

                quantiles = df.approxQuantile(column, [0.25, 0.5, 0.75], 0.05)
                dist_dict["quantiles"] = {
                    "Q1": quantiles[0],
                    "median": quantiles[1],
                    "Q3": quantiles[2]
                }
            else:
                # nenumericke kolone (sto ne ukljucuje 'explicit' kolonu i kolone iz 'categorical_columns')
                # konretno je to samo kolona 'track_name'
                count_value = df.filter(df[column].isNotNull()).count()
                dist_dict = {
                    "count": count_value,
                    "mean": None,
                    "stddev": None,
                    "min": None,
                    "max": None
                }

            # obrada kategorickih kolona
            if column in categorical_columns:
                freq_df = df.groupBy(column).count()
                freq = {row[column]: row["count"] for row in freq_df.collect()}
                dist_dict["frequency"] = freq

            distribution[column] = dist_dict

        except Exception as e:
            dist_dict["error"] = str(e)

    with open("../opos/output/distribution.json", "w", encoding="utf-8") as f:
        json.dump(distribution, f, indent=4, ensure_ascii=False)

    print("Distribution saved to output/distribution.json")
   

# 1
def top_10_collaborations(df):
    # artists, count, avg_collab_popularity
    collab_stats = (
        df.filter(F.col("artists").contains(";"))
        .withColumn("artist_list", F.expr("transform(split(artists, ';'), x -> trim(x))"))
        .withColumn("artist_count", F.size(F.col("artist_list")))
        .filter(F.col("artist_count") >= 2)
        # standardizovanje redoslijeda umjetnika (npr. X feat Y i Y feat X nije isto, ali u analizi kolaboracija treba da se tretiraju kao 2 kolaboracije istih izvodjaca)
        .withColumn("sorted_artists", F.array_sort("artist_list"))
        .withColumn("artists", F.concat_ws(";", "sorted_artists"))
        .groupBy("artists")
        .agg(
            F.count("*").alias("count"),
            F.avg("popularity").alias("avg_collab_popularity")
        )
        .orderBy(F.col("count").desc()).limit(10)
    )
    
    # artist, avg_solo_popularity
    solo_popularity = (
        df.filter(~F.col("artists").contains(";"))
        .withColumn("artist", F.trim(F.col("artists")))
        .groupBy("artist")
        .agg(F.avg("popularity").alias("avg_solo_popularity"))
    )
    
    # artists, least_popular_artist, avg_solo_popularity
    best_match = (
        collab_stats.withColumn("artist", F.explode(F.split(F.col("artists"), ";")))
        .join(solo_popularity, on="artist", how="left")
        .groupBy("artists")
        .agg(
            F.min_by("artist", "avg_solo_popularity").alias("least_popular_artist"),
            F.min("avg_solo_popularity").alias("avg_solo_popularity")
        )
    )
    
    # artists, count, avg_collab_popularity, least_popular_artist, avg_solo_popularity, popularity_diff
    final = (
        collab_stats.join(best_match, on="artists", how="left")
        .withColumn("popularity_diff", F.when(F.col("avg_solo_popularity").isNotNull(), F.col("avg_collab_popularity") - F.col("avg_solo_popularity")))
    )
    
    final.select("artists", "least_popular_artist", "count", "avg_collab_popularity", "avg_solo_popularity", "popularity_diff").orderBy(F.col("avg_collab_popularity").desc()).write.mode("overwrite").json("../opos/output/result_1.json")
    
    single_json_output("result_1.json","result_1_final.json")
    print("Top 10 collaborations analysis saved to output/result_1_final.json")


# 2
def breakthrough(df):
    # album_name, album_avg_popularity
    low_popularity_albums = (
        df.groupBy("album_name")
        .agg(F.avg("popularity").alias("album_avg_popularity"))
        .filter(F.col("album_avg_popularity") < 50)
    )
    
    # zadrzavamo samo informacije o pjesmama sa albuma cija je popularnost < 50
    # sve kolone iz df + album_avg_popularity
    df_filtered = df.join(low_popularity_albums, on="album_name", how="inner")
    
    breakthrough_songs = df_filtered.filter(F.col("popularity") > 80)
    
    # album_name, avg_energy_breakthrough, avg_danceability_breakthrough, avg_valence_breakthrough
    breakthrough_stats = (
        breakthrough_songs.groupBy("album_name")
        .agg(
            F.avg("energy").alias("avg_energy_breakthrough"),
            F.avg("danceability").alias("avg_danceability_breakthrough"),
            F.avg("valence").alias("avg_valence_breakthrough"),
        )
    )
    
    # album_name, avg_energy_other, avg_danceability_other, avg_valence_other
    other_songs_stats = (
        df_filtered.filter(F.col("popularity") <= 80)
        .groupBy("album_name")
        .agg(
            F.avg("energy").alias("avg_energy_other"),
            F.avg("danceability").alias("avg_danceability_other"),
            F.avg("valence").alias("avg_valence_other")
        )
    )
    
    # album_name, breakthrough_songs_info
    breakthrough_song_list = (
        breakthrough_songs.select("album_name", "track_name", "popularity").distinct()
        .withColumn("song_popularity", F.concat_ws(" (popularity: ", "track_name", F.col("popularity").cast("string")))
        .withColumn("song_popularity", F.concat_ws(")", F.col("song_popularity"), F.lit("")))
        .groupBy("album_name")
        .agg(
            F.concat_ws(", ", F.collect_list("song_popularity")).alias("breakthrough_songs_info")
        )
    )
    
    result = (
        breakthrough_stats.join(other_songs_stats, on="album_name", how="inner")
        .withColumn("energy_diff", (F.col("avg_energy_breakthrough") - F.col("avg_energy_other")))
        .withColumn("danceability_diff", (F.col("avg_danceability_breakthrough") - F.col("avg_danceability_other")))
        .withColumn("valence_diff", (F.col("avg_valence_breakthrough") - F.col("avg_valence_other")))
    )
    
    # album_name, avg_energy_breakthrough, avg_danceability_breakthrough, avg_valence_breakthrough, avg_energy_other, avg_danceability_other, avg_valence_other, energy_diff, danceability_diff, valence_diff, breakthrough_songs_info
    final_result_with_songs = result.join(breakthrough_song_list, on="album_name", how="left")
    
    # album_name, album_avg_popularity, avg_energy_breakthrough, avg_danceability_breakthrough, avg_valence_breakthrough, avg_energy_other, avg_danceability_other, avg_valence_other, energy_diff, danceability_diff, valence_diff, breakthrough_songs_info
    final_result_with_songs = (
        final_result_with_songs.join(low_popularity_albums, on="album_name", how="inner")
        .select("album_name", "album_avg_popularity", "breakthrough_songs_info", "avg_energy_breakthrough", "avg_energy_other", "energy_diff", "avg_danceability_breakthrough", "avg_danceability_other", "danceability_diff", "avg_valence_breakthrough", "avg_valence_other", "valence_diff")
        .orderBy(F.col("album_avg_popularity").desc())
        .coalesce(1).write.mode("overwrite").json("../opos/output/result_2.json")
    )
    
    single_json_output("result_2.json","result_2_final.json")
    print("Breakthrough songs analysis saved to output/result_2_final.json")

   
# 3
def tempo_sweet_spot(df):
    # track_genre, avg_genre_popularity
    top_genres = (
        df.groupBy("track_genre")
        .agg(F.avg("popularity").alias("avg_genre_popularity"))
        .orderBy(F.col("avg_genre_popularity").desc()).limit(5)
    )
    
    # zadrzavamo samo informacije o 5 najpopularnijih zanrova
    df_top = df.join(top_genres, on="track_genre", how="inner")
    
    # track_genre, avg_genre_popularity, tempo_range, song_count, avg_song_popularity
    result = (
        df_top.withColumn("tempo_range", F.when(F.col("tempo") < 100, "low").when((F.col("tempo") >= 100) & (F.col("tempo") <= 120), "mid").otherwise("high"))
        .groupBy("track_genre", "tempo_range", "avg_genre_popularity")
        .agg(
            F.count("*").alias("song_count"),
            F.avg("popularity").alias("avg_song_popularity")
        )
        .orderBy(F.col("avg_genre_popularity").desc(), F.col("avg_song_popularity").desc())
    )
    
    result.coalesce(1).write.mode("overwrite").json("../opos/output/result_3.json")
    single_json_output("result_3.json","result_3_final.json")
    print("'Sweet spot' analysis saved to output/result_3_final.json")

     
# 4
def explicit_popularity_correlation(df):
    # track_genre, non_explicit_avg, explicit_avg, non_explicit_count, explicit_count, popularity_diff, total
    result = (
        df.groupBy("track_genre", "explicit")
        .agg(
            F.avg("popularity").alias("avg_popularity"),
            F.count("*").alias("song_count")
        )
        .groupBy("track_genre")
        .pivot("explicit", [False, True])
        .agg(
            F.first("avg_popularity").alias("avg_popularity"),
            F.first("song_count").alias("song_count")
        )
        .withColumnRenamed("False_avg_popularity", "non_explicit_avg")
        .withColumnRenamed("True_avg_popularity", "explicit_avg")
        .withColumnRenamed("False_song_count", "non_explicit_count")
        .withColumnRenamed("True_song_count", "explicit_count")
        .withColumn("popularity_diff", F.col("explicit_avg") - F.col("non_explicit_avg"))
        .withColumn("total", F.col("explicit_count") + F.col("non_explicit_count"))
        .filter((F.col("explicit_count") >= 20) & (F.col("non_explicit_count") >= 20) & (F.col("explicit_count") / F.col("total") >= 0.1) & (F.col("non_explicit_count") / F.col("total") >= 0.1))
    )
    
    # track_genre, explicit_popularity_corr, explicit_effect
    corr_df = (
        df.withColumn("explicit_numeric", F.col("explicit").cast("int"))
        .groupBy("track_genre")
        .agg(
            F.corr("explicit_numeric", "popularity").alias("explicit_popularity_corr")
        )
        .withColumn("explicit_effect", F.when(F.col("explicit_popularity_corr").isNull(), "unknown")
                                        .when(F.col("explicit_popularity_corr") >= 0.3, "strong_positive")
                                        .when(F.col("explicit_popularity_corr") >= 0.1, "weak_positive")
                                        .when(F.col("explicit_popularity_corr") <= -0.3, "strong_negative")
                                        .when(F.col("explicit_popularity_corr") <= -0.1, "weak_negative")
                                        .otherwise("neutral"))
    )
    
    # track_genre, non_explicit_avg, explicit_avg, non_explicit_count, explicit_count, popularity_diff, total, explicit_popularity_corr, explicit_effect
    final_result = result.join(corr_df, on="track_genre", how="left")
    final_result.select("track_genre", "explicit_count", "non_explicit_count", "explicit_avg", "non_explicit_avg", "popularity_diff", "explicit_popularity_corr", "explicit_effect").orderBy(F.col("explicit_popularity_corr").desc_nulls_last()).write.mode("overwrite").json("../opos/output/result_4.json")
    
    single_json_output("result_4.json","result_4_final.json")
    print("Explicit-popularity correlation analysis saved to output/result_4_final.json")
   
   
# 5
def duration_popularity(df):
    # samo pjesme koje za danceability imaju vrijednost vecu od 0.8 od kojih se uzima 10 sa najduzim trajanjem
    songs = (
        df.filter(F.col("danceability") > 0.8)
        .orderBy(F.col("duration_ms").desc()).limit(10)
    )
    
    # track_genre, avg_genre_popularity
    avg_genre_popularity = (
        df.groupBy("track_genre")
        .agg(F.avg("popularity").alias("avg_genre_popularity"))
    )
    
    # sve kolone iz df + avg_genre_popularity, duration_min, popularity_diff, is_more_popular_than_genre_avg uz odabir samo relevantnih kolona u posljednjem koraku
    comparison_df = (
        songs.join(avg_genre_popularity, on="track_genre", how="left")
        .withColumn("duration_min", F.round(F.col("duration_ms") / 60000, 3))
        .withColumn("popularity_diff", F.col("popularity") - F.col("avg_genre_popularity"))
        .withColumn("is_more_popular_than_genre_avg", F.col("popularity_diff") > 0)
        .orderBy(F.desc("duration_min"), F.desc("danceability"))
        .select("track_name", "track_genre", "duration_min", "danceability", "popularity", "avg_genre_popularity", "popularity_diff", "is_more_popular_than_genre_avg")
    )
    
    comparison_df.write.mode("overwrite").json("../opos/output/result_5.json")
    single_json_output("result_5.json","result_5_final.json")
    print("Duration-popularity analysis saved to output/result_5_final.json")
 
  
# 6
def explicit_valence_popularity(df):
    df = df.withColumn("popularity_bin", F.floor(F.col("popularity")/10) * 10)
    
    # popularity_bin, explicit_count, non_explicit_count, avg_valence_explicit, avg_valence_non_explicit, delta_explicit_vs_non_explicit, relative_delta
    delta_valence = (
        # racunanje prosjecne vrijednosti za valence po binu i po eksiplictnosti
        df.groupBy("popularity_bin", "explicit")
        .agg(
            F.avg("valence").alias("avg_valence"),
            F.count("*").alias("count")
        )
        # svaki bin = nova kolona (samo jedna)
        .groupBy("popularity_bin")
        .pivot("explicit", [True, False])
        .agg(
            F.first("avg_valence").alias("avg_valence"),
            F.first("count").alias("count")
        )
        .withColumnRenamed("True_avg_valence", "avg_valence_explicit")
        .withColumnRenamed("False_avg_valence", "avg_valence_non_explicit")
        .withColumnRenamed("True_count", "explicit_count")
        .withColumnRenamed("False_count", "non_explicit_count")
        .withColumn("delta_explicit_vs_non_explicit", F.col("avg_valence_explicit") - F.col("avg_valence_non_explicit"))
        # relative_delta oznacava koliko su eksplicitne pjesme u prosjeku pozitivnije od neeksplicitnih
        .withColumn("relative_delta", F.when(F.col("avg_valence_non_explicit") != 0, F.col("delta_explicit_vs_non_explicit") / F.col("avg_valence_non_explicit")).otherwise(None))
        .select("popularity_bin", "explicit_count", "non_explicit_count", "avg_valence_explicit", "avg_valence_non_explicit", "delta_explicit_vs_non_explicit", "relative_delta")
        .orderBy(F.col("popularity_bin"))
    )
    
    delta_valence.write.mode("overwrite").json("../opos/output/result_6.json")
    single_json_output("result_6.json","result_6_final.json")
    print("Explicit-valence-popularity analysis saved to output/result_6_final.json")
   
    
# 7
def genre_popularity(df):
    # artists, avg_popularity, stddev_popularity, song_count
    artist_stats = (
        df.groupBy("artists")
        .agg(
            F.avg("popularity").alias("avg_popularity"),
            F.stddev("popularity").alias("stddev_popularity"),
            F.count("*").alias("song_count")
        )
        .filter((F.col("stddev_popularity").isNotNull()))
        # umjetnike sa manje od 5 pjesama i popularnoscu manjom od 10 smatramo irelevatnim
        .filter((F.col("avg_popularity") > 10) & (F.col("song_count") >= 5))
        .orderBy(F.col("stddev_popularity")).limit(50)
    )
    
    # sadrzi sve informacije o izdvojenim izvodjacima + avg_popularity, stddev_popularity, song_count
    df_top = df.join(artist_stats, on="artists", how="inner")
    
    # artists, avg_popularity, stddev_popularity, song_count, genres, num_genres
    diversity = (
        df_top.groupBy("artists", "avg_popularity", "stddev_popularity", "song_count")
        .agg(
            F.collect_set("track_genre").alias("genres")
        )
        .withColumn("num_genres", F.size("genres"))
        .orderBy(F.col("stddev_popularity"), F.col("num_genres").desc(), F.col("avg_popularity").desc())
    )
    
    diversity.write.mode("overwrite").json("../opos/output/result_7.json")
    single_json_output("result_7.json","result_7_final.json")
    print("Popularity-genre analysis saved to output/result_7_final.json")
   
   
# 8
def instrumental_spechiness_popularity(df):
    # track_genre, avg_acousticness, avg_instrumentalness, avg_speechiness, avg_popularity, track_count
    genre_audio_features = (
        df.groupBy("track_genre")
        .agg(
            F.avg("acousticness").alias("avg_acousticness"),
            F.avg("instrumentalness").alias("avg_instrumentalness"),
            F.avg("speechiness").alias("avg_speechiness"),
            F.avg("popularity").alias("avg_popularity"),
            F.count("*").alias("track_count")
        )
    )
    
    # track_genre, avg_acousticness, avg_instrumentalness, avg_speechiness, avg_popularity, track_count, category
    instrumental_genres = (
        genre_audio_features.filter((F.col("avg_acousticness") > 0.8) | (F.col("avg_instrumentalness") > 0.8))
        .withColumn("category", F.lit("instrumental"))
    )
    
    vocal_genres = (
        genre_audio_features.filter(F.col("avg_speechiness") > 0.33)
        .withColumn("category", F.lit("vocal"))
    )
    
    # category, avg_category_popularity, num_genres, genres_in_group
    result = (
        instrumental_genres.unionByName(vocal_genres).groupBy("category")
        .agg(
            F.avg("avg_popularity").alias("avg_category_popularity"),
            F.count("*").alias("num_genres"),
            F.collect_list("track_genre").alias("genres_in_group")
        )
        .orderBy(F.col("avg_category_popularity").desc())
    )
    
    result.write.mode("overwrite").json("../opos/output/result_8.json")
    single_json_output("result_8.json","result_8_final.json")
    print("Instrumentalness-speachiness-popularity analysis saved to output/result_8_final.json")
    
    
     
if __name__ == "__main__":
    spark = get_spark_session()
    df = load_data(spark)
    
    df.printSchema()
    df_no_null = df.dropna()
    
    preprocessing(df_no_null)
    top_10_collaborations(df_no_null)
    breakthrough(df_no_null)
    tempo_sweet_spot(df_no_null)
    explicit_popularity_correlation(df_no_null)
    duration_popularity(df_no_null)
    explicit_valence_popularity(df_no_null)
    genre_popularity(df_no_null)
    instrumental_spechiness_popularity(df_no_null)
    
    spark.stop()
