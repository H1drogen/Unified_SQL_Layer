from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HudiExample") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \



columns = ["timestamp","uuid","rider_id","driver_id","fare","city"]
data = [
    (1695159649087,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
    (1695091554788,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
    (1695046462179,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
    (1695516137016,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"),
    (1695115999911,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai")
]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Hudi table options
hudi_options = {
    "hoodie.table.name": "hudi_example_table",
    "hoodie.datasource.write.recordkey.field": "uuid",
    "hoodie.datasource.write.precombine.field": "timestamp",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "hudi_example_table",
    "hoodie.datasource.hive_sync.mode": "hms"
}

# Write DataFrame to Hudi table
df.write.format("hudi") \
    .options(**hudi_options) \
    .mode("overwrite") \
    .save("file:///tmp/hudi_example_table")

# def insert_data_to_table(table_name, data_frame):
#     """
#     Inserts data into the specified table in the database.
#     """
#     # Check if the table exists
#     if spark._jsparkSession.catalog().tableExists(table_name):
#         # Append data to the existing table
#         data_frame.write.format("delta").mode("append").saveAsTable(table_name)
#     else:
#         # Create a new table and insert data
#         data_frame.write.format("delta").mode("overwrite").saveAsTable(table_name)
#
# # Insert data into the Hudi table
# insert_data_to_table("hudi_example_table", df)