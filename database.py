from pyspark.shell import spark
from pyspark.sql.functions import lit, col


columns = ["timestamp","uuid","rider_id","driver_id","fare","city"]
data = [
    (1695159649087,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
    (1695091554788,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
    (1695046462179,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
    (1695516137016,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"),
    (1695115999911,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai")
]
inserts = spark.createDataFrame(data, columns)

def insert_data_to_table(table_name, data_frame):
    """
    Inserts data into the specified table in the database.
    """
    # Check if the table exists
    if spark._jsparkSession.catalog().tableExists(table_name):
        # Append data to the existing table
        data_frame.write.format("delta").mode("append").saveAsTable(table_name)
    else:
        # Create a new table and insert data
        data_frame.write.format("delta").mode("overwrite").saveAsTable(table_name)

insert_data_to_table("rider-data", inserts)