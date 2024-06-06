from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col,to_timestamp, hour,split,count
import pandas
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter

# Create a Spark session
spark = SparkSession.builder.appName("YourAppName").getOrCreate()

# Specify the path to your CSV file
csv_file_path = "final_output.csv"

# Read the CSV file into a DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)










crashes_analysis = df.groupBy("Location").agg(F.count("*").alias("crashes_count"))

# Select the required columns
crashes_analysis = crashes_analysis.select("Location", "crashes_count")
crashes_analysis.show()

# Most location has accidents
location_crash_counts = df.groupBy("Location").count()




# Find the location with the maximum number of crashes
most_common_location = location_crash_counts.orderBy(F.col("count").desc()).first()

# Extract the most common location and count of crashes
most_common_location_name = most_common_location["Location"]
count_of_crashes = most_common_location["count"]

# print(f"The location with the most crashes is '{most_common_location_name}' with {count_of_crashes} crashes.")





# arrange the location has accident in descending order
location_crash_counts = df.groupBy("Location").count().orderBy(F.col("count").desc())
data_to_insert = location_crash_counts.select("Location", "count").collect()
data_tuples = [(row["Location"], row["count"]) for row in data_to_insert]









# Worst airplane type

most_common_type = df.groupBy("Type").count().orderBy(col("count").desc()).select("Type").first()

# print(f"The most common type of airplane involved in accidents is: {most_common_type['Type']}")








# worst types and number of accidents
sorted_types = df.groupBy("Type").count().orderBy(col("count").desc())
pandas_df = sorted_types.toPandas()










# time when crashes happen 
df = df.withColumn("Timestamp", to_timestamp(col("Time"), "HH:mm"))

# Filter out rows where the 'Time' is null
df = df.filter(col("Time").isNotNull())

# Group by timestamp and count the occurrences
crash_frequency = df.groupBy("Time").count().orderBy(col("count").desc())

# crash_frequency.show()
















# average time of crashes 
split_col = split(df['Time'], ':')
df = df.withColumn('Hour', split_col.getItem(0).cast("int"))
df = df.withColumn('Minute', split_col.getItem(1).cast("int"))

df = df.withColumn("TotalMinutes", df['Hour'] * 60 + df['Minute'])

average_minutes = df.selectExpr("avg(TotalMinutes)").collect()[0][0]

average_hours = int(average_minutes // 60)
average_minutes = int(average_minutes % 60)




# get each issue with each model in database

unique_models = df.select('Type').distinct()

# Define a function to process summaries and identify recurring issues
stop_words = set(stopwords.words('english'))

def process_summary(summary):
    tokens = word_tokenize(summary.lower())
    filtered_tokens = [word for word in tokens if word.isalnum() and word not in stop_words]
    return filtered_tokens

# Dictionary to store words associated with each model
model_word_dict = {}


iteration =0;

# Process each unique aircraft model
for model_row in unique_models.collect():
    iteration += 1
    if iteration > 20:
            break  
        
        
    
    model = model_row['Type']
    
    
    # Filter the DataFrame for a specific aircraft model
    model_data = df.filter(col('Type') == model)
    
    # Extract summary strings for this model
    model_summaries = model_data.select('Summary').rdd.flatMap(lambda x: x).collect()
    
    # Process summaries to identify recurring words or issues
    words = []
    for summary in model_summaries:

        
        tokens = process_summary(summary)
        if tokens:
            words.extend(tokens)
    
    # Store recurring words for the model
    model_word_dict[model] = Counter(words).most_common(10)  # Adjust this based on your requirements

    print(f"Model: {model}")
    print("Recurring Issues:")
    # print(model_word_dict[model][0][0])  # Adjust this based on how you've processed and stored the issues
    # print("-------------------")