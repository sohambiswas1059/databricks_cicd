from databricks.connect import DatabricksSession


spark = DatabricksSession.builder.getOrCreate()

environments = ["dev", "test", "prod"]
schemas = ["bronze", "silver", "gold", "landing"]

# Create Catalogs
for env in environments:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS citybike_{env}")

# Create Schemas
for env in environments:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS citybike_{env}.{schema}")

# Create Volumes
for env in environments:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS citybike_{env}.landing.source_citibike_data")