from pyspark.sql.functions import avg, count, round
import dlt

catalog = spark.conf.get("catalog")

@dlt.table(
    name = f"{catalog}.default.daily_station_performance",
    comment = "Daily station performance metrics"
)
def daily_station_performance():
    df = spark.read.format("delta").table(f"{catalog}.default.jc_citybike_silver")

    df = df.\
        groupBy("started_date", "start_station_name").\
        agg(
        round(avg("trip_duration_mins"),2).alias("avg_trip_duration_mins"),
        count("ride_id").alias("total_trips")
    )

    return df