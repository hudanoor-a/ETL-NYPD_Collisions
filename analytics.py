import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.colors as mcolors
from cycler import cycler

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast


# hdfs paths
BASE_HDFS = "hdfs://localhost:9000/warehouse/processed_optimized/nypd_collisions/"
FACT_PATH = BASE_HDFS + "fact_crash/"
DIM_DATE_PATH = BASE_HDFS + "dim_date/"
DIM_LOCATION_PATH = BASE_HDFS + "dim_location/"
DIM_VEHICLE_PATH = BASE_HDFS + "dim_vehicle/"
DIM_FACTOR_PATH = BASE_HDFS + "dim_factor/"

OUTPUT_DIR = Path("m3_outputs")
CHART_DIR = OUTPUT_DIR / "charts"
CSV_DIR = OUTPUT_DIR / "csv"
TEXT_DIR = OUTPUT_DIR / "text"


def print_section(title: str) -> None:
    print(f"\n{title}")


def print_kv(label: str, value) -> None:
    print(f"{label:<42}: {value}")


def ensure_output_dirs() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    CHART_DIR.mkdir(exist_ok=True)
    CSV_DIR.mkdir(exist_ok=True)
    TEXT_DIR.mkdir(exist_ok=True)


def save_df_csv(pdf: pd.DataFrame, filename: str) -> None:
    pdf.to_csv(CSV_DIR / filename, index=False)

MY_COLORS = ["#123740", "#549aab", "#b0d7e1", "#f6f6f6", "#f1802d"]

def apply_professional_style():
    plt.rcParams.update({
        "axes.prop_cycle": cycler(color=MY_COLORS), # This is the magic line
        "figure.figsize": (12, 6),
        "axes.grid": True,
        "grid.alpha": 0.25,
        "grid.linestyle": "--",
        "axes.titlesize": 14,
        "axes.labelsize": 11,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "figure.autolayout": True
    })


def save_bar_chart(pdf: pd.DataFrame, x_col: str, y_col: str,
                   title: str, xlabel: str, ylabel: str, filename: str,
                   rotate=45):
    apply_professional_style()
    plt.figure()
    # No color argument needed; it pulls from prop_cycle
    plt.bar(pdf[x_col].astype(str), pdf[y_col])
    plt.title(title, pad=12)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rotate, ha="right")
    plt.savefig(CHART_DIR / filename, dpi=180, bbox_inches="tight")
    plt.close()

def save_line_chart(pdf: pd.DataFrame, x_col: str, y_col: str,
                    title: str, xlabel: str, ylabel: str, filename: str,
                    rotate=45):
    apply_professional_style()
    plt.figure()
    # It will use the first color (#123740) for the line
    plt.plot(pdf[x_col].astype(str), pdf[y_col], marker="o", linewidth=2)
    plt.title(title, pad=12)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rotate, ha="right")
    plt.savefig(CHART_DIR / filename, dpi=180, bbox_inches="tight")
    plt.close()

def save_stacked_bar_chart(pdf: pd.DataFrame, category_col: str, series_col: str,
                           value_col: str, title: str, xlabel: str, ylabel: str,
                           filename: str):
    apply_professional_style()
    pivot = pdf.pivot(index=category_col, columns=series_col, values=value_col).fillna(0)
    # Pandas .plot() will now use the custom prop_cycle automatically
    pivot.plot(kind="bar", stacked=True, figsize=(12, 6))
    plt.title(title, pad=12)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha="right")
    plt.savefig(CHART_DIR / filename, dpi=180, bbox_inches="tight")
    plt.close()

def save_heatmap(pdf: pd.DataFrame, index_col: str, column_col: str, value_col: str,
                 title: str, xlabel: str, ylabel: str, filename: str):
    apply_professional_style()

    pivot = pdf.pivot(index=index_col, columns=column_col, values=value_col).fillna(0)
    # 1. Define your custom hex colors
    colors = ["#b01111", "#b4451f", "#dd9f40", "#e7d87d", "#62a1db"]
    # 2. Create the colormap
    custom_cmap = mcolors.LinearSegmentedColormap.from_list("custom_theme", colors)
    plt.figure(figsize=(12, 7))
    # 3. Apply the cmap to imshow
    im = plt.imshow(pivot.values, aspect="auto", cmap=custom_cmap)
    plt.colorbar(im, label=value_col)
    plt.title(title, pad=12)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    
    # Handling tick overlap with rotation and alignment
    plt.xticks(range(len(pivot.columns)), pivot.columns, rotation=45, ha='right')
    plt.yticks(range(len(pivot.index)), pivot.index)
    
    plt.savefig(CHART_DIR / filename, dpi=180, bbox_inches="tight")
    plt.close()

def save_geo_scatter(pdf: pd.DataFrame, lat_col: str, lon_col: str,
                     title: str, filename: str):
    apply_professional_style()
    plt.figure(figsize=(8, 8))
    # Scatter also respects the color cycle
    plt.scatter(pdf[lon_col], pdf[lat_col], s=1, alpha=0.15)
    plt.title(title, pad=12)
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.savefig(CHART_DIR / filename, dpi=180, bbox_inches="tight")
    plt.close()

def save_summary_dashboard(q1_pdf: pd.DataFrame, q2_heatmap_pdf: pd.DataFrame, q4_pdf: pd.DataFrame, filename: str):
    apply_professional_style()
    fig, axes = plt.subplots(3, 1, figsize=(14, 14))

    q1_top = q1_pdf.sort_values("fatality_rate_per_1000_crashes", ascending=False).head(10)
    axes[0].bar(q1_top["borough"].astype(str) + "-" + q1_top["year"].astype(str),
                q1_top["fatality_rate_per_1000_crashes"])
    axes[0].set_title("Top Borough-Year Fatality Rates")
    axes[0].set_xlabel("Borough-Year")
    axes[0].set_ylabel("Fatality Rate per 1000")
    axes[0].tick_params(axis="x", rotation=45)
    axes[0].grid(True, alpha=0.25, linestyle="--")

    q2_hour = q2_heatmap_pdf.groupby("hour", as_index=False)["total_pedestrian_injuries"].sum()
    axes[1].plot(q2_hour["hour"].astype(str), q2_hour["total_pedestrian_injuries"], marker="o", linewidth=2)
    axes[1].set_title("Pedestrian Injuries by Hour")
    axes[1].set_xlabel("Hour")
    axes[1].set_ylabel("Pedestrian Injuries")
    axes[1].grid(True, alpha=0.25, linestyle="--")

    q4_plot = q4_pdf.copy()
    q4_plot["x_index"] = range(len(q4_plot))

    axes[2].plot(q4_plot["x_index"], q4_plot["total_cyclist_injuries"], linewidth=2)
    axes[2].set_title("Monthly Cyclist Injury Trend")
    axes[2].set_xlabel("Year")
    axes[2].set_ylabel("Cyclist Injuries")

    year_ticks = q4_plot.groupby("year")["x_index"].min().reset_index()
    axes[2].set_xticks(year_ticks["x_index"])
    axes[2].set_xticklabels(year_ticks["year"].astype(str), rotation=45, ha="right")

    axes[2].grid(True, alpha=0.25, linestyle="--")
    fig.suptitle("Summary Dashboard: Fatality, Pedestrian, and Cyclist Trends", fontsize=16)
    plt.tight_layout()
    plt.savefig(CHART_DIR / filename, dpi=180, bbox_inches="tight")
    plt.close()


def time_sql_query(spark, query: str):
    start = time.perf_counter()
    df = spark.sql(query)
    pdf = df.toPandas()
    elapsed = time.perf_counter() - start
    return pdf, elapsed


def write_text(lines, filename: str):
    with open(TEXT_DIR / filename, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line.rstrip() + "\n")

def write_chart_interpretations():
    chart_interpretations = [
        "Q1 - Borough Fatality Rates Year-over-Year:\n"
        "Shows how fatality rates shift across boroughs over different years. "
        "Boroughs with high rates in certain years likely had more severe crashes relative to their total volume, "
        "which is more useful than just looking at raw crash counts.\n",

        "Q2 - Pedestrian Injuries Heatmap (Day vs Hour):\n"
        "Shows which day/hour combinations are worst for pedestrian injuries. "
        "The darker cells are the time slots where we'd want to focus enforcement or awareness efforts.\n",

        "Q3 - Fatal vs Non-Fatal Crashes by Borough:\n"
        "Stacked bars make it easy to see both total crash volume and how much of it involves fatalities. "
        "Some boroughs might have fewer total crashes but a higher share of fatal ones.\n",

        "Q4 - Monthly Cyclist Injuries Since 2012:\n"
        "Tracks how cyclist injuries fluctuate month by month over the years. "
        "You can see seasonal patterns and whether things have gotten better or worse over time.\n",

        "Q5 - Top Factors in Multi-Vehicle 3+ Injury Crashes:\n"
        "Ranks the most common contributing factors in serious multi-vehicle crashes. "
        "The tallest bars are the factors that keep showing up in the worst crashes - useful for targeting awareness campaigns.\n",

        "Bonus - Geographic Crash Density:\n"
        "Scatter plot of crash locations across NYC. Dense clusters point to specific streets or intersections "
        "that might need infrastructure improvements.\n",

        "Summary Dashboard:\n"
        "Combines the fatality rate, pedestrian injury, and cyclist injury charts into one view. "
        "Good for getting a quick overview without flipping between charts.\n"
    ]

    write_text(chart_interpretations, "chart_interpretations.txt")
    
def add_interpretation(lines, title, sentences):
    lines.append(title)
    for sentence in sentences:
        lines.append(f"- {sentence}")
    lines.append("")


def main():
    ensure_output_dirs()

    spark = SparkSession.builder \
        .appName("nypd_collisions_m3_analytics_final") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print_section("starting analytics...")

    # load tables from hdfs
    fact = spark.read.parquet(FACT_PATH)
    dim_date = spark.read.parquet(DIM_DATE_PATH)
    dim_location = spark.read.parquet(DIM_LOCATION_PATH)
    dim_vehicle = spark.read.parquet(DIM_VEHICLE_PATH)
    dim_factor = spark.read.parquet(DIM_FACTOR_PATH)

    print_section("table row counts")
    print_kv("fact_crash rows", fact.count())
    print_kv("dim_date rows", dim_date.count())
    print_kv("dim_location rows", dim_location.count())
    print_kv("dim_vehicle rows", dim_vehicle.count())
    print_kv("dim_factor rows", dim_factor.count())
    print_section("checking partitions")

    partition_columns_present = (
        "partition_year" in fact.columns and "partition_month" in fact.columns
    )

    print_kv("Partition columns detected", "YES" if partition_columns_present else "NO")

    if partition_columns_present:
        partition_summary = fact.select("partition_year", "partition_month").distinct() \
            .orderBy("partition_year", "partition_month")
        partition_count = partition_summary.count()

        print_kv("Distinct year-month partitions", partition_count)
        partition_summary.show(20, truncate=False)

        partition_preview_pdf = partition_summary.toPandas()
        save_df_csv(partition_preview_pdf, "partition_summary.csv")

    # cache everything and broadcast the smaller dims
    fact.cache()
    dim_date.cache()
    dim_location.cache()
    dim_vehicle.cache()
    dim_factor.cache()

    analytics_base = fact.alias("f") \
        .join(broadcast(dim_date).alias("d"), col("f.date_key") == col("d.date_key"), "left") \
        .join(broadcast(dim_location).alias("l"), col("f.location_key") == col("l.location_key"), "left") \
        .join(broadcast(dim_vehicle).alias("v"), col("f.vehicle_key") == col("v.vehicle_key"), "left") \
        .join(broadcast(dim_factor).alias("fa"), col("f.factor_key") == col("fa.factor_key"), "left") \
        .select(
            col("f.collision_id"),
            col("f.persons_injured"),
            col("f.persons_killed"),
            col("f.pedestrians_injured"),
            col("f.pedestrians_killed"),
            col("f.cyclists_injured"),
            col("f.cyclists_killed"),
            col("f.motorists_injured"),
            col("f.motorists_killed"),
            col("f.property_damage_only"),
            col("f.injury_count_flagged"),

            col("d.crash_date"),
            col("d.crash_time"),
            col("d.hour"),
            col("d.day_of_week"),
            col("d.month"),
            col("d.year"),
            col("d.is_weekend"),

            col("l.borough"),
            col("l.zip_code"),
            col("l.on_street_name"),
            col("l.latitude"),
            col("l.longitude"),

            col("v.vehicle_type_1"),
            col("v.vehicle_type_2"),
            col("v.vehicle_type_3"),
            col("v.vehicle_type_4"),
            col("v.vehicle_type_5"),

            col("fa.factor_1"),
            col("fa.factor_2"),
            col("fa.factor_3"),
            col("fa.factor_4"),
            col("fa.factor_5"),
            col("f.partition_year"),
            col("f.partition_month")
        )

    analytics_base.cache()
    analytics_base.createOrReplaceTempView("analytics_base")

    print_section("analytics base ready")
    print_kv("joined rows", analytics_base.count())

    timing_notes = []
    timing_query = """
        SELECT borough, COUNT(*) AS total_crashes
        FROM analytics_base
        WHERE borough IS NOT NULL
        GROUP BY borough
        ORDER BY total_crashes DESC
    """

    uncached_pdf, uncached_time = time_sql_query(spark, timing_query)
    cached_pdf, cached_time = time_sql_query(spark, timing_query)

    timing_notes.append("caching + broadcast join timing:")
    timing_notes.append(f"first run: {uncached_time:.4f} seconds")
    timing_notes.append(f"second run (cached): {cached_time:.4f} seconds")
    timing_notes.append("tables and analytics_base were cached in memory.")
    timing_notes.append("broadcast joins used for dimension tables.")
    timing_notes.append("fact table read from partitioned parquet (partition_year, partition_month).")

    print_section("performance timing")
    print_kv("First execution", f"{uncached_time:.4f} sec")
    print_kv("Second execution", f"{cached_time:.4f} sec")
    print_kv("Caching used", "YES")
    print_kv("Broadcast joins used", "YES")
    print_kv(
        "Partitioned parquet detected",
        "YES" if ("partition_year" in fact.columns and "partition_month" in fact.columns) else "NO"
    )

    insights = []

    print_section("Q1 - Borough Fatality Rates by Year")

    q1_sql = """
        -- Q1: Calculate borough fatality rates year-over-year.
        -- Window function RANK() ranks boroughs by fatality rate within each year.
        WITH borough_year AS (
            SELECT
                borough,
                year,
                COUNT(*) AS total_crashes,
                SUM(persons_killed) AS total_fatalities,
                CASE
                    WHEN COUNT(*) > 0 THEN (SUM(persons_killed) * 1000.0) / COUNT(*)
                    ELSE 0
                END AS fatality_rate_per_1000_crashes
            FROM analytics_base
            WHERE borough IS NOT NULL
              AND year IS NOT NULL
              AND year BETWEEN 2012 AND 2025
            GROUP BY borough, year
        )
        SELECT
            borough,
            year,
            total_crashes,
            total_fatalities,
            fatality_rate_per_1000_crashes,
            RANK() OVER (
                PARTITION BY year
                ORDER BY fatality_rate_per_1000_crashes DESC
            ) AS fatality_rank_within_year
        FROM borough_year
        ORDER BY borough, year
    """
    q1_pdf, _ = time_sql_query(spark, q1_sql)
    save_df_csv(q1_pdf, "q1_borough_fatality_rates.csv")
    print(q1_pdf.head(15).to_string(index=False))

    q1_chart = q1_pdf.copy()
    q1_chart["borough_year"] = q1_chart["borough"].astype(str) + "-" + q1_chart["year"].astype(str)
    save_line_chart(
        q1_chart.head(40),
        "borough_year",
        "fatality_rate_per_1000_crashes",
        "Q1: Borough Fatality Rates Year-over-Year",
        "Borough-Year",
        "Fatality Rate per 1000 Crashes",
        "q1_borough_fatality_rate.png",
        rotate=45
    )

    if not q1_pdf.empty:
        top_q1 = q1_pdf.sort_values("fatality_rate_per_1000_crashes", ascending=False).head(1).iloc[0]
        add_interpretation(insights, "Q1 Interpretation", [
            f"The highest borough-year fatality rate was {top_q1['borough']} in {int(top_q1['year'])}, with {top_q1['fatality_rate_per_1000_crashes']:.2f} fatalities per 1000 crashes.",
            "This indicates that risk should not be evaluated only by total crash count; severity rate also matters.",
            "Borough-year combinations with high fatality rates can be prioritized for safety audits and targeted enforcement.",
            "The ranking column helps identify the most severe borough in each year using a Spark SQL window function."
        ])

    print_section("Q2 - Peak Hours for Pedestrian Injuries")

    q2_sql = """
        SELECT
            hour,
            day_of_week,
            factor_1 AS contributing_factor,
            SUM(pedestrians_injured) AS total_pedestrian_injuries
        FROM analytics_base
        WHERE hour IS NOT NULL
          AND day_of_week IS NOT NULL
          AND factor_1 IS NOT NULL
        GROUP BY hour, day_of_week, factor_1
        ORDER BY total_pedestrian_injuries DESC
    """
    q2_pdf, _ = time_sql_query(spark, q2_sql)
    save_df_csv(q2_pdf, "q2_peak_hours_pedestrian_injuries.csv")
    print(q2_pdf.head(15).to_string(index=False))

    q2_heatmap_sql = """
        SELECT
            day_of_week,
            hour,
            SUM(pedestrians_injured) AS total_pedestrian_injuries
        FROM analytics_base
        WHERE hour IS NOT NULL
          AND day_of_week IS NOT NULL
        GROUP BY day_of_week, hour
        ORDER BY day_of_week, hour
    """
    q2_heatmap_pdf, _ = time_sql_query(spark, q2_heatmap_sql)
    save_df_csv(q2_heatmap_pdf, "q2_hour_day_heatmap.csv")

    save_heatmap(
        q2_heatmap_pdf,
        "day_of_week",
        "hour",
        "total_pedestrian_injuries",
        "Q2: Pedestrian Injuries Heatmap by Day and Hour",
        "Hour of Day",
        "Day of Week",
        "q2_pedestrian_injuries_heatmap.png"
    )

    if not q2_pdf.empty:
        top_q2 = q2_pdf.head(1).iloc[0]
        add_interpretation(insights, "Q2 Interpretation", [
            f"The peak pedestrian injury combination was hour {int(top_q2['hour'])}, day_of_week {int(top_q2['day_of_week'])}, with factor '{top_q2['contributing_factor']}'.",
            f"This combination recorded {int(top_q2['total_pedestrian_injuries'])} pedestrian injuries.",
            "The heatmap helps identify recurring high-risk time periods for pedestrian safety planning.",
            "A business or city agency could use this result to time awareness campaigns, crossing guard deployment, or targeted traffic enforcement."
        ])

    print_section("Q3 - Vehicle Type vs Fatal/Non-Fatal by Borough")

    q3_sql = """
        SELECT
            borough,
            vehicle_type_1,
            CASE
                WHEN persons_killed > 0 THEN 'Fatal'
                ELSE 'Non-Fatal'
            END AS crash_severity,
            COUNT(*) AS crash_count
        FROM analytics_base
        WHERE borough IS NOT NULL
          AND vehicle_type_1 IS NOT NULL
        GROUP BY
            borough,
            vehicle_type_1,
            CASE
                WHEN persons_killed > 0 THEN 'Fatal'
                ELSE 'Non-Fatal'
            END
        ORDER BY crash_count DESC
    """
    q3_pdf, _ = time_sql_query(spark, q3_sql)
    save_df_csv(q3_pdf, "q3_vehicle_fatal_nonfatal_by_borough.csv")
    print(q3_pdf.head(15).to_string(index=False))

    q3_chart_pdf = q3_pdf.groupby(["borough", "crash_severity"], as_index=False)["crash_count"].sum()
    save_stacked_bar_chart(
        q3_chart_pdf.head(20),
        "borough",
        "crash_severity",
        "crash_count",
        "Q3: Fatal vs Non-Fatal Crash Breakdown by Borough",
        "Borough",
        "Crash Count",
        "q3_fatal_nonfatal_stacked.png"
    )

    if not q3_pdf.empty:
        top_q3 = q3_pdf.head(1).iloc[0]
        add_interpretation(insights, "Q3 Interpretation", [
            f"The most frequent borough/vehicle/severity combination was {top_q3['borough']} with vehicle type '{top_q3['vehicle_type_1']}' and severity '{top_q3['crash_severity']}'.",
            f"This group recorded {int(top_q3['crash_count'])} crashes.",
            "The stacked bar chart separates fatal and non-fatal outcomes, making severity patterns easier to compare across boroughs.",
            "This can help transportation planners focus vehicle-specific safety interventions in boroughs with the largest crash burden."
        ])

    print_section("Q4 - Cyclist Injury Trends (monthly since 2012)")

    q4_sql = """
        -- Q4: Track monthly cyclist injuries and calculate month-over-month changes.
        -- Window function LAG() compares each month with the previous month.
        WITH monthly AS (
            SELECT
                year,
                month,
                SUM(cyclists_injured) AS total_cyclist_injuries,
                CASE
                    WHEN year >= 2019 THEN 'Post-2019'
                    ELSE 'Pre-2019'
                END AS period
            FROM analytics_base
            WHERE year IS NOT NULL
              AND month IS NOT NULL
              AND year >= 2012
            GROUP BY year, month, CASE WHEN year >= 2019 THEN 'Post-2019' ELSE 'Pre-2019' END
        )
        SELECT
            year,
            month,
            total_cyclist_injuries,
            period,
            LAG(total_cyclist_injuries) OVER (ORDER BY year, month) AS previous_month_injuries,
            total_cyclist_injuries - LAG(total_cyclist_injuries) OVER (ORDER BY year, month) AS month_over_month_change
        FROM monthly
        ORDER BY year, month
    """
    q4_pdf, _ = time_sql_query(spark, q4_sql)
    q4_pdf["year_month"] = q4_pdf["year"].astype(str) + "-" + q4_pdf["month"].astype(str).str.zfill(2)
    save_df_csv(q4_pdf, "q4_cyclist_injury_trends_monthly.csv")
    print(q4_pdf.head(15).to_string(index=False))

    save_line_chart(
        q4_pdf,
        "year_month",
        "total_cyclist_injuries",
        "Q4: Cyclist Injury Trends Monthly Since 2012",
        "Year-Month",
        "Cyclist Injuries",
        "q4_cyclist_injury_trend.png",
        rotate=90
    )

    q4_period_sql = """
        SELECT
            CASE
                WHEN year >= 2019 THEN 'Post-2019'
                ELSE 'Pre-2019'
            END AS period,
            SUM(cyclists_injured) AS total_cyclist_injuries,
            AVG(cyclists_injured) AS avg_row_cyclist_injuries
        FROM analytics_base
        WHERE year IS NOT NULL
          AND year >= 2012
        GROUP BY CASE WHEN year >= 2019 THEN 'Post-2019' ELSE 'Pre-2019' END
        ORDER BY period
    """
    q4_period_pdf, _ = time_sql_query(spark, q4_period_sql)
    save_df_csv(q4_period_pdf, "q4_pre_post_2019_summary.csv")
    print(q4_period_pdf.to_string(index=False))

    add_interpretation(insights, "Q4 Interpretation", [
        "The monthly cyclist injury trend shows how cyclist crash injury patterns change over time.",
        "The LAG() window function calculates month-over-month changes, highlighting months where cyclist injuries increased or decreased sharply.",
        "The pre/post-2019 comparison helps evaluate whether cyclist injury patterns shifted after 2019.",
        "Transportation agencies can use these trends to evaluate bicycle-lane planning, seasonal safety campaigns, and enforcement timing."
    ])

    print_section("Q5 - Factors in Multi-Vehicle 3+ Injury Crashes")

    q5_sql = """
        -- Q5: Rank contributing factors in multi-vehicle crashes with 3+ injuries by time of day.
        -- Window function ROW_NUMBER() ranks factors inside each time-of-day group.
        WITH factor_counts AS (
            SELECT
                factor_1 AS contributing_factor,
                CASE
                    WHEN hour BETWEEN 5 AND 11 THEN 'Morning'
                    WHEN hour BETWEEN 12 AND 16 THEN 'Afternoon'
                    WHEN hour BETWEEN 17 AND 20 THEN 'Evening'
                    ELSE 'Night'
                END AS time_of_day,
                COUNT(*) AS crash_count
            FROM analytics_base
            WHERE persons_injured >= 3
              AND vehicle_type_2 IS NOT NULL
              AND factor_1 IS NOT NULL
              AND hour IS NOT NULL
            GROUP BY
                factor_1,
                CASE
                    WHEN hour BETWEEN 5 AND 11 THEN 'Morning'
                    WHEN hour BETWEEN 12 AND 16 THEN 'Afternoon'
                    WHEN hour BETWEEN 17 AND 20 THEN 'Evening'
                    ELSE 'Night'
                END
        )
        SELECT
            contributing_factor,
            time_of_day,
            crash_count,
            ROW_NUMBER() OVER (
                PARTITION BY time_of_day
                ORDER BY crash_count DESC
            ) AS factor_rank_within_time_of_day
        FROM factor_counts
        ORDER BY crash_count DESC
    """
    q5_pdf, _ = time_sql_query(spark, q5_sql)
    save_df_csv(q5_pdf, "q5_multi_vehicle_3plus_injuries_factors.csv")
    print(q5_pdf.head(15).to_string(index=False))

    save_bar_chart(
        q5_pdf.head(15),
        "contributing_factor",
        "crash_count",
        "Q5: Top Contributing Factors in Multi-Vehicle 3+ Injury Crashes",
        "Contributing Factor",
        "Crash Count",
        "q5_multi_vehicle_factors.png",
        rotate=45
    )

    if not q5_pdf.empty:
        top_q5 = q5_pdf.head(1).iloc[0]
        add_interpretation(insights, "Q5 Interpretation", [
            f"The top factor in multi-vehicle crashes with 3+ injuries was '{top_q5['contributing_factor']}' during '{top_q5['time_of_day']}'.",
            f"This group recorded {int(top_q5['crash_count'])} crashes.",
            "The ROW_NUMBER() window function ranks contributing factors within each time-of-day group.",
            "The result supports targeted safety actions by identifying when severe multi-vehicle crashes are most associated with specific contributing factors."
        ])

    print_section("bonus - geographic crash density")

    geo_sql = """
        SELECT latitude, longitude
        FROM analytics_base
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
        LIMIT 50000
    """
    geo_pdf, _ = time_sql_query(spark, geo_sql)
    save_df_csv(geo_pdf, "bonus_geo_density_points.csv")
    save_geo_scatter(
        geo_pdf,
        "latitude",
        "longitude",
        "Geographic Crash Density Scatter",
        "bonus_geo_density_scatter.png"
    )
    print_kv("Geo points plotted", len(geo_pdf))

    print_section("summary dashboard")
    save_summary_dashboard(
        q1_pdf,
        q2_heatmap_pdf,
        q4_pdf,
        "summary_dashboard.png"
    )
    print_kv("Dashboard chart", "summary_dashboard.png")

    print_section("writing output files")
    write_text(insights, "insights_summary.txt")
    write_text(timing_notes, "performance_notes.txt")
    write_chart_interpretations()
    print_kv("Insights summary", TEXT_DIR / "insights_summary.txt")
    print_kv("Performance notes", TEXT_DIR / "performance_notes.txt")
    print_kv("Chart interpretations", TEXT_DIR / "chart_interpretations.txt")

    print_section("all done! outputs:")
    print_kv("CSV output folder", CSV_DIR)
    print_kv("Chart output folder", CHART_DIR)
    print_kv("Text output folder", TEXT_DIR)
    print_kv("Q1 chart", "q1_borough_fatality_rate.png")
    print_kv("Q2 chart", "q2_pedestrian_injuries_heatmap.png")
    print_kv("Q3 chart", "q3_fatal_nonfatal_stacked.png")
    print_kv("Q4 chart", "q4_cyclist_injury_trend.png")
    print_kv("Q5 chart", "q5_multi_vehicle_factors.png")
    print_kv("Bonus geo chart", "bonus_geo_density_scatter.png")
    print_kv("Summary dashboard", "summary_dashboard.png")
    print_kv("Partition summary CSV", "partition_summary.csv")
    print_kv("Chart interpretations", "chart_interpretations.txt")
    print_kv("Status", "SUCCESS")
    spark.stop()


if __name__ == "__main__":
    main()