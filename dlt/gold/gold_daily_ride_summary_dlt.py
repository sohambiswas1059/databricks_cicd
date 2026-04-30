from pyspark.sql.functions import max, min, avg, count, round
import dlt

catalog = spark.conf.get("catalog")

@dlt.table(
    name=f"{catalog}.default.daily_ride_summary",
    comment="Daily summary of bike rides with max, min, avg trip duration and total trips."
)
def daily_ride_summary():    
    df = spark.read.format("delta").table(f"{catalog}.default.jc_citybike_silver")

    df = df.groupBy("started_date").agg(
    round(max("trip_duration_mins"),2).alias("max_trip_duration_mins"),
    round(min("trip_duration_mins"),2).alias("min_trip_duration_mins"),
    round(avg("trip_duration_mins"),2).alias("avg_trip_duration_mins"),
    count("ride_id").alias("total_trips")
    )
    
    return df