from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, FloatType
import pymysql

# Function to insert data into MySQL
def insert_into_phpmyadmin(row, analysis_type):
    host = "localhost"
    port = 3306
    database = "big_data"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Extract the required columns from the row
    if analysis_type == "crashes_analysis":
        column1_value = row.Location
        column2_value = row.crashes_count
        sql_query = f"INSERT INTO location_stream (location, count) VALUES ('{column1_value}', '{column2_value}')"
    elif analysis_type == "max_aboard":
        column1_value = row.Location
        column2_value = row.max_aboard
        sql_query = f"INSERT INTO number of deaths (location, max_aboard) VALUES ('{column1_value}', '{column2_value}')"

    # Execute the SQL query
    cursor.execute(sql_query)
    print("Inserted in DB")

    # Commit the changes
    conn.commit()
    conn.close()

# Create a Spark session
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Define the schema and read data from Kafka
schema = (StructType()
    .add("Date", StringType())
    .add("Time", StringType())
    .add("Location", StringType())
    .add("Route", StringType())
    .add("Type", StringType())
    .add("Aboard", FloatType())
)

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "big") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))

my_df = df.select("data.*")

# Calculate crash counts per location
crashes_analysis = my_df.groupBy("Location").agg(count("*").alias("crashes_count"))

# Calculate the maximum number of people aboard per location
max_aboard = my_df.groupBy("Location").agg(max("Aboard").alias("max_aboard"))

# Start streaming query and write crash counts to MySQL
query_crashes_analysis = crashes_analysis.writeStream \
    .outputMode("complete") \
    .format("console") \
    .foreach(lambda row: insert_into_phpmyadmin(row, "crashes_analysis")) \
    .start()

# Start streaming query and write max number of people aboard to MySQL
query_max_aboard = max_aboard.writeStream \
    .outputMode("complete") \
    .format("console") \
    .foreach(lambda row: insert_into_phpmyadmin(row, "max_aboard")) \
    .start()

query_crashes_analysis.awaitTermination()
query_max_aboard.awaitTermination()
