from test import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType , FloatType
import pymysql
import pandas as pd


#conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
#cursor = conn.cursor()
host = "localhost"
port = 3306
database = "big_data"
username = "root"
password = ""

def insert_into_phpmyadmin(row):
    # Define the connection details for your PHPMyAdmin database

    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()


    sql_query = f"INSERT INTO plane(Date, Time, Location, Operator, Flight,Route, Type, Registration, cn, Aboard, Fatalities, Ground)VALUES ('{row.Date}', '{row.Time}', '{row.Location}', '{row.Operator}', '{row.Flight}', '{row.Route}', '{row.Type}', '{row.Registration}', '{row.cn}', '{row.Aboard}', '{row.Fatalities}' , '{row.Ground}')"
    # Execute the SQL query
    cursor.execute(sql_query)
    

    # Commit the changes
    conn.commit()
    conn.close()

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = (StructType()
    .add("Date", StringType())
    .add("Time", StringType())
    .add("Location", StringType())
    .add("Operator", StringType())
    .add("Flight", StringType())
    .add("Route", StringType())
    .add("Type", StringType())
    .add("Registration", StringType())
    .add("cn", StringType()) 
    .add("Aboard", FloatType())
    .add("Fatalities", FloatType())
    .add("Ground", FloatType())
    .add("Summary",StringType())
    )


# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "big") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \

# Select specific columns from "data"
df = df.select("data.*")


conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
cursor = conn.cursor()
sql_query2 = f"INSERT INTO most_location(Location) VALUES('{most_common_location_name}')"
sql = "INSERT INTO locations (location, number) VALUES (%s, %s)"

# Insert data into MySQL
try:
    cursor.executemany(sql, data_tuples)
    conn.commit()
    print("Data inserted successfully!")
except Exception as e:
    conn.rollback()
    print(f"Error: {str(e)}")





sql_query3 = f"INSERT INTO worst_type(type) VALUES('{most_common_type['Type']}')"




cursor.execute(sql_query2)
cursor.execute(sql_query3)


for index, row in pandas_df.iterrows():
    airplane_type = row['Type']
    accident_count = row['count']
    sql_query_4 = f"INSERT INTO accidents (type, crashes) VALUES ('{airplane_type}', {accident_count})"
    cursor.execute(sql_query_4)


for row in crash_frequency.collect():
    try:
        timestamp_value = row["Time"]
        count_value = row["count"]
        sql_query = f"INSERT INTO crash_frequency (time, accidents) VALUES (%s, %s)"
        cursor.execute(sql_query, (timestamp_value, count_value))
    except Exception as e:
        print(f"Error inserting data: {e}")
        
        

sql_query6 = f"INSERT INTO average_crashes_time (average_hr,average_min) VALUES('{average_hours}','{average_minutes}')"
cursor.execute(sql_query6)



for model, issue in model_word_dict.items():
    model_name = model
    recurring_issue = issue[0][0] if issue else None  # Get the most recurring issue
    
    # Prepare the SQL query
    sql_query = f"INSERT INTO model_issues(model, issue) VALUES ('{model_name}', '{recurring_issue}')"
    
    # Execute the SQL query
    cursor.execute(sql_query)



conn.commit()
conn.close()









# Convert the value column to string and display the result
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreach(insert_into_phpmyadmin) \
    .start()

# Wait for the query to finish
query.awaitTermination()