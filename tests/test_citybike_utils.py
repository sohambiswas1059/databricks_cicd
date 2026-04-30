# test_citibike_utils.py


import datetime
from pyspark.sql import Row
from src.citibike.citibike_utils import *


NULL_INTERPOLATION_SCHEMA = """
ride_id string,
rideable_type string,
member_casual string,
started_at timestamp,
ended_at timestamp
"""

def test_get_trip_duration_mins(spark):

            
    # Create a test DataFrame with known start and end timestamps using datetime objects
    data = [
        (datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 10, 0)),  # 10 minutes
        (datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 30, 0))   # 30 minutes
    ]
    schema = "start_timestamp timestamp, end_timestamp timestamp"
  
    
    df = spark.createDataFrame(data, schema=schema)
    
    # Apply the function to calculate trip duration in minutes
    result_df = get_trip_duration_mins(spark, df, "start_timestamp", "end_timestamp", "trip_duration_mins")
    
    # Collect the results for assertions
    results = result_df.select("trip_duration_mins").take(2)
    
    # Assert that the differences are as expected
    assert results[0]["trip_duration_mins"] == 10
    assert results[1]["trip_duration_mins"] == 30

def test_timestamp_to_date(spark):
            
    
    data = [(datetime.datetime(2025, 4, 10, 10, 30, 0),)]
    schema = "ride_timestamp timestamp"
    df = spark.createDataFrame(data, schema=schema)
    
    # Use the utility to add a date column
    result_df = timestamp_to_date(spark, df, "ride_timestamp", "ride_date")
    
    # Assert that the extracted date matches the expected value
    row = result_df.select("ride_date").first()

    expected_date = datetime.date(2025, 4, 10)  # Expected: 2025-04-10

    assert row["ride_date"] == expected_date


def test_fill_null_timestamps_forward(spark):
    """Nulls should be filled using the last non-null value (forward fill)"""
    t1 = datetime.datetime(2025, 4, 10, 10, 0, 0)
    t2 = datetime.datetime(2025, 4, 10, 11, 0, 0)

    data = [Row(started_at=t1), Row(started_at=None), Row(started_at=t2)]
    df = spark.createDataFrame(data)

    result = fill_null_timestamps(df, "started_at").collect()

    assert result[1]["started_at"] == t1  # filled forward from row 0


def test_fill_null_timestamps_backward(spark):
    """If no prior value exists, null should be filled from next non-null (backward fill)"""
    t1 = datetime.datetime(2025, 4, 10, 10, 0, 0)

    data = [Row(started_at=None), Row(started_at=t1)]
    df = spark.createDataFrame(data)

    result = fill_null_timestamps(df, "started_at").collect()

    assert result[0]["started_at"] == t1 


def test_null_interpolation_fills_ride_id(spark):
    """Null ride_id should be replaced with a generated uuid"""
    data = [Row(ride_id=None, rideable_type="classic_bike", member_casual="member",
                started_at=datetime.datetime(2025, 4, 10, 10, 0, 0),
                ended_at=datetime.datetime(2025, 4, 10, 10, 30, 0))]
    df = spark.createDataFrame(data, schema=NULL_INTERPOLATION_SCHEMA)

    result = null_interpolation(spark, df).collect()

    assert result[0]["ride_id"] is not None


def test_null_interpolation_fills_rideable_type(spark):
    """Null rideable_type should default to 'electric_bike'"""
    data = [Row(ride_id="abc", rideable_type=None, member_casual="member",
                started_at=datetime.datetime(2025, 4, 10, 10, 0, 0),
                ended_at=datetime.datetime(2025, 4, 10, 10, 30, 0))]
    df = spark.createDataFrame(data, schema=NULL_INTERPOLATION_SCHEMA)

    result = null_interpolation(spark, df).collect()

    assert result[0]["rideable_type"] == "electric_bike"


def test_null_interpolation_fills_member_casual(spark):
    """Null member_casual should default to 'casual'"""
    data = [Row(ride_id="abc", rideable_type="classic_bike", member_casual=None,
                started_at=datetime.datetime(2025, 4, 10, 10, 0, 0),
                ended_at=datetime.datetime(2025, 4, 10, 10, 30, 0))]
    df = spark.createDataFrame(data, schema=NULL_INTERPOLATION_SCHEMA)

    result = null_interpolation(spark, df).collect()

    assert result[0]["member_casual"] == "casual"
