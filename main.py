import os

from reverse_geocoder import tree
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, StringType, LongType
import pyspark.sql.functions  as F

if __name__ == '__main__':

    # Creation of the search tree for reverse geocoding operations
    rev_tree = tree.GeocodeData(os.path.join(os.getcwd(), 'reverse_geocoder', 'point.csv'))

    # Definition of a "User Definition Function" for reverse geocoding operations
    query_udf = F.udf(lambda a,b : ','.join(rev_tree.query((a,b))), StringType() )

    # Creating the spark session
    spark = SparkSession \
        .builder \
        .appName("Test with csv file") \
        .getOrCreate()

    # Define the scheme
    UserSchema = StructType([
        StructField("id_node", IntegerType(), True),
        StructField("timestamp", LongType(), True),
        StructField("id_object", IntegerType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("speed", FloatType(), True),
        StructField("yaw", FloatType(), True),
    ])


    # Load stream from csv file
    df = spark \
        .readStream \
        .option("sep", " ") \
        .schema(UserSchema) \
        .format("csv") \
        .load("stream")

    ############### Writing the raw stream to memory #################
    df.writeStream \
        .queryName("Row stream") \
        .format("parquet") \
        .option("path", os.path.join(os.getcwd(), 'sink', 'sink_stream_raw')) \
        .option("checkpointLocation", os.path.join(os.getcwd(), 'checkpoint', 'checkpoint_stream_raw')) \
        .start()

    df = df.drop(df["id_node"])


    # Add latitude and longitude to the dataframe and cast the timestamp into the TimestampType
    df_modified = df.withColumn("timestamp_modified",
                        F.from_unixtime(df["timestamp"] /1000, format='yyyy-MM-dd HH:mm:ss').cast(TimestampType()))\
        .withColumn("location",
                    query_udf(df["latitude"],df["longitude"]))

    df_modified= df_modified.withColumn("timestamp", F.regexp_extract(df["timestamp"], ".{3}$", 0))
    df_modified = df_modified.withColumn("timestamp_millisecond", F.concat(df_modified["timestamp_modified"], F.lit('.'), df_modified["timestamp"]).cast(TimestampType()))


    # Splitting the column into different columns using Spark's split function 
    split_col = F.split(df_modified["location"], ',')
    df_modified = df_modified.withColumn("name", split_col.getItem(0))\
        .withColumn("highway", split_col.getItem(1))\
        .withColumn("lanes", split_col.getItem(2))\
        .withColumn("bridge", split_col.getItem(3))\
        .withColumn("lit", split_col.getItem(4))\
        .withColumn("id", split_col.getItem(5))\
        .withColumn("unique_id", split_col.getItem(6))


    # Calculation of the density of vehicles divided by section and type of vehicle 
    query = df_modified \
        .withWatermark("timestamp_millisecond", "2 minutes") \
        .groupBy("timestamp_millisecond", "name", "unique_id", "id_object")\
        .agg(
            F.count("*").alias("count"),
            F.avg("speed").alias("average_speed"),
            )


    ############### Writing the stream modified to memory #################
    query.writeStream\
        .queryName("streamingOutput")\
        .format("parquet")\
        .option("path", os.path.join(os.getcwd(), 'sink', 'sink_stream_modified'))\
        .option("checkpointLocation", os.path.join(os.getcwd(), 'checkpoint', 'checkpoint_stream_modified'))\
        .start()



    ##################### Aggregation 1 minute ####################
    # Load the previously modified stream
    df_read = spark \
        .readStream \
        .schema(query.schema) \
        .parquet(os.path.join(os.getcwd(), 'sink', 'sink_stream_modified'))

    # Creates aggregates with 1 minute time window
    df_oneminute = df_read\
        .withWatermark("timestamp_millisecond", "2 minutes")\
        .groupBy(F.window("timestamp_millisecond", "1 minute"),
                "name",
                "unique_id",
                "id_object")\
        .agg(F.avg("count").alias("average_count"),
             F.avg("average_speed").alias("average_speed"),)

    

    df_oneminute = df_oneminute.withColumn("window_start", df_oneminute["window.start"]).withColumn("window_end", df_oneminute["window.end"])
    df_oneminute = df_oneminute.drop(df_oneminute["window"])

    # Write the stream to memory
    df_oneminute\
        .writeStream\
        .queryName("Aggregazione 1 minuto") \
        .format("parquet") \
        .option("path", os.path.join(os.getcwd(), 'sink', 'sink_one_minute')) \
        .option("checkpointLocation", os.path.join(os.getcwd(), 'checkpoint', 'checkpoint_one_minute')) \
        .start()

    ##################### Aggregation 15 minute ####################
    # Load the stream with the aggregated data at 1 minute
    df_read1 = spark \
        .readStream \
        .schema(df_oneminute.schema) \
        .parquet(os.path.join(os.getcwd(), 'sink', 'sink_one_minute'))

    # Creates aggregates with 15 minute time window
    df_fifteenminute = df_read1 \
        .withWatermark("window_start", "2 minutes") \
        .groupBy(F.window("window_start", "15 minute"),
                "name",
                "unique_id",
                "id_object") \
        .agg(F.avg("average_count").alias("average_count"),
             F.avg("average_speed").alias("average_speed"), )

    df_fifteenminute = df_fifteenminute.withColumn("window_start", df_fifteenminute["window.start"]).withColumn("window_end",
                                                                                                 df_fifteenminute[
                                                                                                     "window.end"])
    df_fifteenminute = df_fifteenminute.drop(df_fifteenminute["window"])

    # Write the stream to memory
    df_fifteenminute \
        .writeStream \
        .queryName("Aggregazione 15 minuto") \
        .format("parquet") \
        .option("path", os.path.join(os.getcwd(), 'sink', 'sink_fifteen_minute')) \
        .option("checkpointLocation", os.path.join(os.getcwd(), 'checkpoint', 'checkpoint_fifteen_minute')) \
        .start()

    ##################### Aggregation 1 hour ####################
    # Load the stream with the aggregated data at 15 minute
    df_read2 = spark \
        .readStream \
        .schema(df_fifteenminute.schema) \
        .parquet(os.path.join(os.getcwd(), 'sink', 'sink_fifteen_minute'))

    # Creates aggregates with 1 hour time window
    df_hour = df_read2 \
        .withWatermark("window_start", "2 minutes") \
        .groupBy(F.window("window_start", "1 hour"),
                 "name",
                 "unique_id",
                 "id_object") \
        .agg(F.avg("average_count").alias("average_count"),
             F.avg("average_speed").alias("average_speed"), )

    df_hour = df_hour.withColumn("window_start", df_hour["window.start"]).withColumn("window_end",
                                                                                                 df_hour[
                                                                                                     "window.end"])
    df_hour = df_hour.drop(df_hour["window"])

    # Write the stream to memory
    df_hour \
        .writeStream \
        .queryName("Aggregazione 1 ora") \
        .format("parquet") \
        .option("path", os.path.join(os.getcwd(), 'sink', 'sink_one_hour')) \
        .option("checkpointLocation", os.path.join(os.getcwd(), 'checkpoint', 'checkpoint_one_hour')) \
        .start()

    ##################### Aggregation 1 day ####################
    # Load the stream with the aggregated data at 1 hour
    df_read3 = spark \
        .readStream \
        .schema(df_hour.schema) \
        .parquet(os.path.join(os.getcwd(), 'sink', 'sink_one_hour'))

    # Creates aggregates with 1 day time window
    df_day = df_read3 \
        .withWatermark("window_start", "2 minutes") \
        .groupBy(F.window("window_start", "1 day"),
                 "name",
                 "unique_id",
                 "id_object") \
        .agg(F.avg("average_count").alias("average_count"),
             F.avg("average_speed").alias("average_speed"), )


    df_day = df_day.withColumn("window_start", df_day["window.start"]).withColumn("window_end",
                                                                                                 df_day[
                                                                                                     "window.end"])
    df_day = df_day.drop(df_day["window"])

    # Write the stream to memory
    df_day \
        .writeStream \
        .queryName("Aggregazione 1 giorno") \
        .format("parquet") \
        .option("path", os.path.join(os.getcwd(), 'sink', 'sink_one_day')) \
        .option("checkpointLocation", os.path.join(os.getcwd(), 'checkpoint', 'checkpoint_one_day')) \
        .start().awaitTermination()

