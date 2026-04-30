from src.citibike.citibike_utils import get_trip_duration_mins, null_interpolation, timestamp_to_date
from pyspark.sql.functions import create_map, lit
import dlt

catalog = spark.conf.get("catalog")

@dlt.table(
    name = f"{catalog}.default.jc_citybike_silver",
    comment = "Silver table for JC Citybike trip data with transformations"
)
def silver_jc_citybike():
    df = dlt.read(f"{catalog}.default.jc_citybike_bronze")

    df = null_interpolation(spark, df)

    df = get_trip_duration_mins(spark, df, "started_at", "ended_at", "trip_duration_mins")

    df = timestamp_to_date(spark, df, "started_at", "started_date")
    df = timestamp_to_date(spark, df, "ended_at", "ended_date")

    df = df.select(
        "ride_id",
        "started_date",
        "started_at",
        "ended_date",
        "ended_at",
        "start_station_name",
        "end_station_name",
        "trip_duration_mins"
    )

    return df