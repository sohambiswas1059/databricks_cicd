import sys
from src.citibike.citibike_utils import get_trip_duration_mins, null_interpolation, timestamp_to_date
from pyspark.sql.functions import create_map, lit


pipeline_id = sys.argv[1]
run_id = sys.argv[2]
task_id = sys.argv[3]
processed_timestamp = sys.argv[4]
catalog = sys.argv[5]

df= spark.read.format("delta").table(f"{catalog}.bronze.jc_citybike")

df = null_interpolation(spark, df)

df = get_trip_duration_mins(spark, df, "started_at", "ended_at", "trip_duration_mins")

df = timestamp_to_date(spark, df, "started_at", "started_date")
df = timestamp_to_date(spark, df, "ended_at", "ended_date")

df = df.withColumn("metadata", 
              create_map(
                  lit("pipeline_id"), lit(pipeline_id),
                  lit("run_id"), lit(run_id),
                  lit("task_id"), lit(task_id),
                  lit("processed_timestamp"), lit(processed_timestamp)
                  ))

df = df.select(
    "ride_id",
    "started_date",
    "started_at",
    "ended_date",
    "ended_at",
    "start_station_name",
    "end_station_name",
    "trip_duration_mins",
    "metadata"
    )

df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver.jc_citybike")