from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, to_date, to_timestamp, hour, month, year,
    dayofweek, monotonically_increasing_id, concat_ws,
    coalesce, lit, trim, first
)

RAW_PATH = "hdfs://localhost:9000/warehouse/raw/nypd_collisions/year_2026/month_04/"
PROCESSED_BASE_PATH = "hdfs://localhost:9000/warehouse/processed/nypd_collisions/"
FACT_PATH = PROCESSED_BASE_PATH + "fact_crash/"
DIM_DATE_PATH = PROCESSED_BASE_PATH + "dim_date/"
DIM_LOCATION_PATH = PROCESSED_BASE_PATH + "dim_location/"
DIM_VEHICLE_PATH = PROCESSED_BASE_PATH + "dim_vehicle/"
DIM_FACTOR_PATH = PROCESSED_BASE_PATH + "dim_factor/"


def print_section(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def print_kv(label: str, value) -> None:
    print(f"{label:<38}: {value}")


def main():
    spark = SparkSession.builder \
        .appName("nypd_collisions_m2_etl_final") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print_section("M2 ETL PIPELINE STARTED")
    print_kv("Raw path", RAW_PATH)

    # ==========================================================
    # STEP 1: READ RAW DATA
    # ==========================================================
    df_raw = spark.read.csv(
        RAW_PATH,
        header=True,
        inferSchema=False
    )

    raw_row_count = df_raw.count()

    print_section("STEP 1 - RAW DATA SUMMARY")
    print_kv("Read status", "SUCCESS")
    print_kv("Raw row count", raw_row_count)
    print_kv("Column count", len(df_raw.columns))

    # ==========================================================
    # STEP 2: REMOVE NULL/BLANK COLLISION_ID + DEDUPLICATE
    # ==========================================================
    df_id_clean = df_raw.filter(
        col("COLLISION_ID").isNotNull() &
        (trim(col("COLLISION_ID")) != "")
    )

    null_collision_id_removed = raw_row_count - df_id_clean.count()

    df_dedup = df_id_clean.dropDuplicates(["COLLISION_ID"])
    dedup_row_count = df_dedup.count()
    duplicates_removed = df_id_clean.count() - dedup_row_count

    print_section("STEP 2 - ID CLEANING + DEDUPLICATION")
    print_kv("Rows before", raw_row_count)
    print_kv("Null/blank COLLISION_ID removed", null_collision_id_removed)
    print_kv("Rows after ID cleaning", df_id_clean.count())
    print_kv("Rows after dedup", dedup_row_count)
    print_kv("Duplicates removed", duplicates_removed)

    # ==========================================================
    # STEP 3: CLEAN COORDINATES
    # ==========================================================
    df_coords = df_dedup \
        .withColumn("LATITUDE_DBL", col("LATITUDE").cast("double")) \
        .withColumn("LONGITUDE_DBL", col("LONGITUDE").cast("double"))

    zero_coord_count = df_coords.filter(
        (col("LATITUDE_DBL") == 0.0) & (col("LONGITUDE_DBL") == 0.0)
    ).count()

    df_coords = df_coords \
        .withColumn(
            "LATITUDE_DBL",
            when(col("LATITUDE_DBL") == 0.0, None).otherwise(col("LATITUDE_DBL"))
        ) \
        .withColumn(
            "LONGITUDE_DBL",
            when(col("LONGITUDE_DBL") == 0.0, None).otherwise(col("LONGITUDE_DBL"))
        )

    lat_oor_count = df_coords.filter(
        col("LATITUDE_DBL").isNotNull() &
        ((col("LATITUDE_DBL") < 40.4) | (col("LATITUDE_DBL") > 40.9))
    ).count()

    lon_oor_count = df_coords.filter(
        col("LONGITUDE_DBL").isNotNull() &
        ((col("LONGITUDE_DBL") < -74.3) | (col("LONGITUDE_DBL") > -73.7))
    ).count()

    df_coords = df_coords \
        .withColumn(
            "LATITUDE_DBL",
            when(
                (col("LATITUDE_DBL") < 40.4) | (col("LATITUDE_DBL") > 40.9),
                None
            ).otherwise(col("LATITUDE_DBL"))
        ) \
        .withColumn(
            "LONGITUDE_DBL",
            when(
                (col("LONGITUDE_DBL") < -74.3) | (col("LONGITUDE_DBL") > -73.7),
                None
            ).otherwise(col("LONGITUDE_DBL"))
        )

    print_section("STEP 3 - COORDINATE CLEANING")
    print_kv("Zero coord rows found", zero_coord_count)
    print_kv("Latitude OOR rows found", lat_oor_count)
    print_kv("Longitude OOR rows found", lon_oor_count)

    # ==========================================================
    # STEP 4: DATE CLEANING
    # ==========================================================
    df_dates = df_coords.withColumn(
        "CRASH_DATE_PARSED",
        to_date(col("CRASH DATE"), "MM/dd/yyyy")
    )

    invalid_date_count = df_dates.filter(col("CRASH_DATE_PARSED").isNull()).count()
    df_dates = df_dates.filter(col("CRASH_DATE_PARSED").isNotNull())
    rows_after_date_clean = df_dates.count()

    print_section("STEP 4 - DATE CLEANING")
    print_kv("Invalid date rows removed", invalid_date_count)
    print_kv("Rows after date cleaning", rows_after_date_clean)

    # ==========================================================
    # STEP 5: CAST METRICS + FLAGS
    # ==========================================================
    injury_cols = [
        "NUMBER OF PERSONS INJURED",
        "NUMBER OF PERSONS KILLED",
        "NUMBER OF PEDESTRIANS INJURED",
        "NUMBER OF PEDESTRIANS KILLED",
        "NUMBER OF CYCLIST INJURED",
        "NUMBER OF CYCLIST KILLED",
        "NUMBER OF MOTORIST INJURED",
        "NUMBER OF MOTORIST KILLED"
    ]

    df_flags = df_dates
    for c in injury_cols:
        df_flags = df_flags.withColumn(c, col(c).cast("int"))

    df_flags = df_flags.withColumn(
        "property_damage_only",
        (
            (col("NUMBER OF PERSONS INJURED") == 0) &
            (col("NUMBER OF PERSONS KILLED") == 0) &
            (col("NUMBER OF PEDESTRIANS INJURED") == 0) &
            (col("NUMBER OF PEDESTRIANS KILLED") == 0) &
            (col("NUMBER OF CYCLIST INJURED") == 0) &
            (col("NUMBER OF CYCLIST KILLED") == 0) &
            (col("NUMBER OF MOTORIST INJURED") == 0) &
            (col("NUMBER OF MOTORIST KILLED") == 0)
        )
    )

    df_flags = df_flags.withColumn(
        "injury_count_flagged",
        when(col("NUMBER OF PERSONS INJURED") > 20, True).otherwise(False)
    )

    pdo_count = df_flags.filter(col("property_damage_only") == True).count()
    outlier_count = df_flags.filter(col("injury_count_flagged") == True).count()

    print_section("STEP 5 - FLAGS")
    print_kv("PDO rows flagged", pdo_count)
    print_kv("Outlier rows flagged", outlier_count)

    # ==========================================================
    # STEP 6: TIME SUPPORT
    # ==========================================================
    df_time = df_flags.withColumn(
        "CRASH_TIME_FILLED",
        coalesce(trim(col("CRASH TIME")), lit("00:00"))
    ).withColumn(
        "CRASH_TS_STR",
        concat_ws(" ", col("CRASH DATE"), col("CRASH_TIME_FILLED"))
    ).withColumn(
        "CRASH_TS",
        to_timestamp(col("CRASH_TS_STR"), "MM/dd/yyyy H:mm")
    )

    # ==========================================================
    # STEP 7: NORMALIZED JOIN COLUMNS
    # ==========================================================
    df_norm = df_time \
        .withColumn("J_CRASH_TIME", coalesce(trim(col("CRASH TIME")), lit("UNKNOWN_TIME"))) \
        .withColumn("J_BOROUGH", coalesce(trim(col("BOROUGH")), lit("UNKNOWN_BOROUGH"))) \
        .withColumn("J_ZIP_CODE", coalesce(trim(col("ZIP CODE")), lit("UNKNOWN_ZIP"))) \
        .withColumn("J_ON_STREET_NAME", coalesce(trim(col("ON STREET NAME")), lit("UNKNOWN_STREET"))) \
        .withColumn("J_LATITUDE", coalesce(col("LATITUDE_DBL"), lit(-9999.0))) \
        .withColumn("J_LONGITUDE", coalesce(col("LONGITUDE_DBL"), lit(-9999.0))) \
        .withColumn("J_VEHICLE_1", coalesce(trim(col("VEHICLE TYPE CODE 1")), lit("UNKNOWN_VEHICLE"))) \
        .withColumn("J_VEHICLE_2", coalesce(trim(col("VEHICLE TYPE CODE 2")), lit("UNKNOWN_VEHICLE"))) \
        .withColumn("J_VEHICLE_3", coalesce(trim(col("VEHICLE TYPE CODE 3")), lit("UNKNOWN_VEHICLE"))) \
        .withColumn("J_VEHICLE_4", coalesce(trim(col("VEHICLE TYPE CODE 4")), lit("UNKNOWN_VEHICLE"))) \
        .withColumn("J_VEHICLE_5", coalesce(trim(col("VEHICLE TYPE CODE 5")), lit("UNKNOWN_VEHICLE"))) \
        .withColumn("J_FACTOR_1", coalesce(trim(col("CONTRIBUTING FACTOR VEHICLE 1")), lit("UNKNOWN_FACTOR"))) \
        .withColumn("J_FACTOR_2", coalesce(trim(col("CONTRIBUTING FACTOR VEHICLE 2")), lit("UNKNOWN_FACTOR"))) \
        .withColumn("J_FACTOR_3", coalesce(trim(col("CONTRIBUTING FACTOR VEHICLE 3")), lit("UNKNOWN_FACTOR"))) \
        .withColumn("J_FACTOR_4", coalesce(trim(col("CONTRIBUTING FACTOR VEHICLE 4")), lit("UNKNOWN_FACTOR"))) \
        .withColumn("J_FACTOR_5", coalesce(trim(col("CONTRIBUTING FACTOR VEHICLE 5")), lit("UNKNOWN_FACTOR")))

    # ==========================================================
    # STEP 8: BUILD DIM_DATE (UNIQUE ON NORMALIZED JOIN KEY)
    # ==========================================================
    df_date_dim = df_norm.groupBy(
        col("CRASH_DATE_PARSED").alias("crash_date"),
        col("J_CRASH_TIME").alias("j_crash_time")
    ).agg(
        first(col("CRASH TIME"), ignorenulls=True).alias("crash_time"),
        first(hour(col("CRASH_TS")), ignorenulls=True).alias("hour"),
        first(dayofweek(col("CRASH_DATE_PARSED")), ignorenulls=True).alias("day_of_week"),
        first(month(col("CRASH_DATE_PARSED")), ignorenulls=True).alias("month"),
        first(year(col("CRASH_DATE_PARSED")), ignorenulls=True).alias("year")
    ).withColumn(
        "is_weekend",
        when(col("day_of_week").isin(1, 7), True).otherwise(False)
    )

    df_date_dim = df_date_dim.withColumn(
        "date_key",
        monotonically_increasing_id() + 1
    ).select(
        "date_key", "crash_date", "crash_time", "hour",
        "day_of_week", "month", "year", "is_weekend", "j_crash_time"
    )

    dim_date_count = df_date_dim.count()

    print_section("STEP 6 - DIM_DATE")
    print_kv("dim_date rows", dim_date_count)

    # ==========================================================
    # STEP 9: BUILD DIM_LOCATION (UNIQUE ON NORMALIZED JOIN KEY)
    # ==========================================================
    df_location_dim = df_norm.groupBy(
        col("J_BOROUGH").alias("j_borough"),
        col("J_ZIP_CODE").alias("j_zip_code"),
        col("J_ON_STREET_NAME").alias("j_on_street_name"),
        col("J_LATITUDE").alias("j_latitude"),
        col("J_LONGITUDE").alias("j_longitude")
    ).agg(
        first(col("BOROUGH"), ignorenulls=True).alias("borough"),
        first(col("ZIP CODE"), ignorenulls=True).alias("zip_code"),
        first(col("ON STREET NAME"), ignorenulls=True).alias("on_street_name"),
        first(col("LATITUDE_DBL"), ignorenulls=True).alias("latitude"),
        first(col("LONGITUDE_DBL"), ignorenulls=True).alias("longitude")
    ).withColumn(
        "geo_imputed",
        lit(False)
    )

    df_location_dim = df_location_dim.withColumn(
        "location_key",
        monotonically_increasing_id() + 1
    ).select(
        "location_key", "borough", "zip_code", "on_street_name",
        "latitude", "longitude", "geo_imputed",
        "j_borough", "j_zip_code", "j_on_street_name",
        "j_latitude", "j_longitude"
    )

    dim_location_count = df_location_dim.count()

    print_section("STEP 7 - DIM_LOCATION")
    print_kv("dim_location rows", dim_location_count)

    # ==========================================================
    # STEP 10: BUILD DIM_VEHICLE (UNIQUE ON NORMALIZED JOIN KEY)
    # ==========================================================
    df_vehicle_dim = df_norm.groupBy(
        col("J_VEHICLE_1").alias("j_vehicle_1"),
        col("J_VEHICLE_2").alias("j_vehicle_2"),
        col("J_VEHICLE_3").alias("j_vehicle_3"),
        col("J_VEHICLE_4").alias("j_vehicle_4"),
        col("J_VEHICLE_5").alias("j_vehicle_5")
    ).agg(
        first(col("VEHICLE TYPE CODE 1"), ignorenulls=True).alias("vehicle_type_1"),
        first(col("VEHICLE TYPE CODE 2"), ignorenulls=True).alias("vehicle_type_2"),
        first(col("VEHICLE TYPE CODE 3"), ignorenulls=True).alias("vehicle_type_3"),
        first(col("VEHICLE TYPE CODE 4"), ignorenulls=True).alias("vehicle_type_4"),
        first(col("VEHICLE TYPE CODE 5"), ignorenulls=True).alias("vehicle_type_5")
    )

    df_vehicle_dim = df_vehicle_dim.withColumn(
        "vehicle_key",
        monotonically_increasing_id() + 1
    ).select(
        "vehicle_key",
        "vehicle_type_1", "vehicle_type_2", "vehicle_type_3", "vehicle_type_4", "vehicle_type_5",
        "j_vehicle_1", "j_vehicle_2", "j_vehicle_3", "j_vehicle_4", "j_vehicle_5"
    )

    dim_vehicle_count = df_vehicle_dim.count()

    print_section("STEP 8 - DIM_VEHICLE")
    print_kv("dim_vehicle rows", dim_vehicle_count)

    # ==========================================================
    # STEP 11: BUILD DIM_FACTOR (UNIQUE ON NORMALIZED JOIN KEY)
    # ==========================================================
    df_factor_dim = df_norm.groupBy(
        col("J_FACTOR_1").alias("j_factor_1"),
        col("J_FACTOR_2").alias("j_factor_2"),
        col("J_FACTOR_3").alias("j_factor_3"),
        col("J_FACTOR_4").alias("j_factor_4"),
        col("J_FACTOR_5").alias("j_factor_5")
    ).agg(
        first(col("CONTRIBUTING FACTOR VEHICLE 1"), ignorenulls=True).alias("factor_1"),
        first(col("CONTRIBUTING FACTOR VEHICLE 2"), ignorenulls=True).alias("factor_2"),
        first(col("CONTRIBUTING FACTOR VEHICLE 3"), ignorenulls=True).alias("factor_3"),
        first(col("CONTRIBUTING FACTOR VEHICLE 4"), ignorenulls=True).alias("factor_4"),
        first(col("CONTRIBUTING FACTOR VEHICLE 5"), ignorenulls=True).alias("factor_5")
    )

    df_factor_dim = df_factor_dim.withColumn(
        "factor_key",
        monotonically_increasing_id() + 1
    ).select(
        "factor_key",
        "factor_1", "factor_2", "factor_3", "factor_4", "factor_5",
        "j_factor_1", "j_factor_2", "j_factor_3", "j_factor_4", "j_factor_5"
    )

    dim_factor_count = df_factor_dim.count()

    print_section("STEP 9 - DIM_FACTOR")
    print_kv("dim_factor rows", dim_factor_count)

    # ==========================================================
    # STEP 12: PREPARE FACT BASE
    # ==========================================================
    df_fact_base = df_norm.select(
        col("COLLISION_ID").cast("long").alias("collision_id"),
        col("CRASH_DATE_PARSED"),
        col("J_CRASH_TIME"),
        col("J_BOROUGH"),
        col("J_ZIP_CODE"),
        col("J_ON_STREET_NAME"),
        col("J_LATITUDE"),
        col("J_LONGITUDE"),
        col("J_VEHICLE_1"),
        col("J_VEHICLE_2"),
        col("J_VEHICLE_3"),
        col("J_VEHICLE_4"),
        col("J_VEHICLE_5"),
        col("J_FACTOR_1"),
        col("J_FACTOR_2"),
        col("J_FACTOR_3"),
        col("J_FACTOR_4"),
        col("J_FACTOR_5"),
        col("NUMBER OF PERSONS INJURED").alias("persons_injured"),
        col("NUMBER OF PERSONS KILLED").alias("persons_killed"),
        col("NUMBER OF PEDESTRIANS INJURED").alias("pedestrians_injured"),
        col("NUMBER OF PEDESTRIANS KILLED").alias("pedestrians_killed"),
        col("NUMBER OF CYCLIST INJURED").alias("cyclists_injured"),
        col("NUMBER OF CYCLIST KILLED").alias("cyclists_killed"),
        col("NUMBER OF MOTORIST INJURED").alias("motorists_injured"),
        col("NUMBER OF MOTORIST KILLED").alias("motorists_killed"),
        col("property_damage_only"),
        col("injury_count_flagged")
    )

    fact_base_count = df_fact_base.count()

    # ==========================================================
    # STEP 13: JOIN FACT TO DIMENSIONS
    # ==========================================================
    df_fact = df_fact_base.join(
        df_date_dim,
        (df_fact_base["CRASH_DATE_PARSED"] == df_date_dim["crash_date"]) &
        (df_fact_base["J_CRASH_TIME"] == df_date_dim["j_crash_time"]),
        "left"
    ).join(
        df_location_dim,
        (df_fact_base["J_BOROUGH"] == df_location_dim["j_borough"]) &
        (df_fact_base["J_ZIP_CODE"] == df_location_dim["j_zip_code"]) &
        (df_fact_base["J_ON_STREET_NAME"] == df_location_dim["j_on_street_name"]) &
        (df_fact_base["J_LATITUDE"] == df_location_dim["j_latitude"]) &
        (df_fact_base["J_LONGITUDE"] == df_location_dim["j_longitude"]),
        "left"
    ).join(
        df_vehicle_dim,
        (df_fact_base["J_VEHICLE_1"] == df_vehicle_dim["j_vehicle_1"]) &
        (df_fact_base["J_VEHICLE_2"] == df_vehicle_dim["j_vehicle_2"]) &
        (df_fact_base["J_VEHICLE_3"] == df_vehicle_dim["j_vehicle_3"]) &
        (df_fact_base["J_VEHICLE_4"] == df_vehicle_dim["j_vehicle_4"]) &
        (df_fact_base["J_VEHICLE_5"] == df_vehicle_dim["j_vehicle_5"]),
        "left"
    ).join(
        df_factor_dim,
        (df_fact_base["J_FACTOR_1"] == df_factor_dim["j_factor_1"]) &
        (df_fact_base["J_FACTOR_2"] == df_factor_dim["j_factor_2"]) &
        (df_fact_base["J_FACTOR_3"] == df_factor_dim["j_factor_3"]) &
        (df_fact_base["J_FACTOR_4"] == df_factor_dim["j_factor_4"]) &
        (df_fact_base["J_FACTOR_5"] == df_factor_dim["j_factor_5"]),
        "left"
    ).select(
        "collision_id",
        "date_key",
        "location_key",
        "vehicle_key",
        "factor_key",
        "persons_injured",
        "persons_killed",
        "pedestrians_injured",
        "pedestrians_killed",
        "cyclists_injured",
        "cyclists_killed",
        "motorists_injured",
        "motorists_killed",
        "property_damage_only",
        "injury_count_flagged"
    )

    fact_count = df_fact.count()
    fact_distinct_collision_count = df_fact.select("collision_id").distinct().count()

    null_collision_id_count = df_fact.filter(col("collision_id").isNull()).count()
    null_date_key_count = df_fact.filter(col("date_key").isNull()).count()
    null_location_key_count = df_fact.filter(col("location_key").isNull()).count()
    null_vehicle_key_count = df_fact.filter(col("vehicle_key").isNull()).count()
    null_factor_key_count = df_fact.filter(col("factor_key").isNull()).count()

    fact_duplicate_rows = fact_count - fact_base_count
    duplicate_collision_ids = fact_count - fact_distinct_collision_count

    print_section("STEP 10 - FACT_CRASH")
    print_kv("fact base rows", fact_base_count)
    print_kv("fact_crash rows", fact_count)
    print_kv("fact row inflation", fact_duplicate_rows)
    print_kv("duplicate collision_id rows", duplicate_collision_ids)
    print_kv("NULL collision_id rows", null_collision_id_count)
    print_kv("NULL date_key rows", null_date_key_count)
    print_kv("NULL location_key rows", null_location_key_count)
    print_kv("NULL vehicle_key rows", null_vehicle_key_count)
    print_kv("NULL factor_key rows", null_factor_key_count)

    print_section("FACT_CRASH SAMPLE")
    df_fact.show(5, truncate=False, vertical=True)

    # ==========================================================
    # STEP 14: WRITE PARQUET OUTPUTS
    # ==========================================================
    print_section("STEP 11 - WRITE PARQUET TO HDFS")
    print_kv("dim_date path", DIM_DATE_PATH)
    print_kv("dim_location path", DIM_LOCATION_PATH)
    print_kv("dim_vehicle path", DIM_VEHICLE_PATH)
    print_kv("dim_factor path", DIM_FACTOR_PATH)
    print_kv("fact_crash path", FACT_PATH)

    df_date_dim.drop("j_crash_time").write.mode("overwrite").parquet(DIM_DATE_PATH)
    df_location_dim.drop(
        "j_borough", "j_zip_code", "j_on_street_name", "j_latitude", "j_longitude"
    ).write.mode("overwrite").parquet(DIM_LOCATION_PATH)
    df_vehicle_dim.drop(
        "j_vehicle_1", "j_vehicle_2", "j_vehicle_3", "j_vehicle_4", "j_vehicle_5"
    ).write.mode("overwrite").parquet(DIM_VEHICLE_PATH)
    df_factor_dim.drop(
        "j_factor_1", "j_factor_2", "j_factor_3", "j_factor_4", "j_factor_5"
    ).write.mode("overwrite").parquet(DIM_FACTOR_PATH)
    df_fact.write.mode("overwrite").parquet(FACT_PATH)

    print_kv("Write status", "SUCCESS")

    print_section("FINAL ETL SUMMARY")
    print_kv("Raw rows", raw_row_count)
    print_kv("Rows after dedup", dedup_row_count)
    print_kv("Rows after date cleaning", rows_after_date_clean)
    print_kv("dim_date rows", dim_date_count)
    print_kv("dim_location rows", dim_location_count)
    print_kv("dim_vehicle rows", dim_vehicle_count)
    print_kv("dim_factor rows", dim_factor_count)
    print_kv("fact_crash rows", fact_count)
    print_kv("Status", "SUCCESS")

    spark.stop()


if __name__ == "__main__":
    main()