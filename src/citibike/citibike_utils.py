
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def get_trip_duration_mins(spark, df, start_col, end_col, output_col):
    
   return df.withColumn(output_col, timestamp_diff("minute", col(start_col), col(end_col)))


def timestamp_to_date(spark, df, timestamp_col, output_col):
    return df.withColumn(output_col, col(timestamp_col).cast("date"))


def fill_null_timestamps(df, timestamp_col):
    forward_window = Window.orderBy(lit(1)).rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
    backward_window = Window.orderBy(lit(1)).rowsBetween(
        Window.currentRow,
        Window.unboundedFollowing,
    )

    return df.withColumn(
        timestamp_col,
        coalesce(
            last(col(timestamp_col), ignorenulls=True).over(forward_window),
            first(col(timestamp_col), ignorenulls=True).over(backward_window),
        ),
    )


def null_interpolation(spark, df):
    df = df.withColumn(
        "ride_id",
        when(col("ride_id").isNull(), expr("uuid()")).otherwise(col("ride_id")),
    ).withColumn(
        "rideable_type",
        when(col("rideable_type").isNull(), lit("electric_bike")).otherwise(col("rideable_type")),
    ).withColumn(
        "member_casual",
        when(col("member_casual").isNull(), lit("casual")).otherwise(col("member_casual")),
    )

    df = fill_null_timestamps(df, "started_at")
    df = fill_null_timestamps(df, "ended_at")

    return df


