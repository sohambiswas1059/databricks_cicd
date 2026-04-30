from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import create_map, lit
import dlt

schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_name", StringType(), True), 
    StructField("start_station_id", StringType(), True),   
    StructField("end_station_name", StringType(), True), 
    StructField("end_station_id", StringType(), True), 
    StructField("start_lat", DecimalType(), True), 
    StructField("start_lng", DecimalType(), True), 
    StructField("end_lat", DecimalType(), True), 
    StructField("end_lng", DecimalType(), True), 
    StructField("member_casual", StringType(), True), 
])


catalog = spark.conf.get("catalog")


@dlt.table(
    name = f"{catalog}.default.jc_citybike_bronze",
    comment = "Bronze table for JC Citybike trip data",
    table_properties={"overwriteSchema": "true"}
)
def bronze_jc_citybike():
    df = spark.read.csv(f"/Volumes/{catalog}/landing/source_citibike_data/JC-202503-citibike-tripdata.csv", schema=schema, header=True)
    return df
