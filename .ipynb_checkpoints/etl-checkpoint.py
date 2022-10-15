import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql.functions import from_unixtime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


#########################
###### functions ########
#########################

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+"song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT DISTINCT 
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM song_data
        WHERE song_id is not null""")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode('overwrite').parquet(output_data+"songs.parquet")

    # extract columns to create artists table
    artists_table =  spark.sql("""
        SELECT DISTINCT
            artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
        FROM song_data
        WHERE artist_id is not null""")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+"artists.parquet")
    

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+"log_data/2018/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
      
    #convert ts column to date format
    df = df.withColumn('as_date',from_unixtime((col('ts')/1000)))

    # creating a virtual table from logs for sql querying
    df.createOrReplaceTempView("log_data")
    
    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT
            userId,
            firstName,
            lastName,
            gender,
            level
        FROM log_data
        where userId is not null
        order by userId""")
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+"users.parquet")

    # extract columns to create time table
    time_table = spark.sql("""
        SELECT 
            ts,
            hour(as_date) as hour,
            dayofmonth(as_date) as day,
            weekofyear(as_date) as week,
            month(as_date) as month,
            year(as_date) as year,
            dayofweek(as_date) as weekday
        FROM log_data""")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode('overwrite').parquet(output_data+"time.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT
            month(log_data.as_date) as month,
            year(log_data.as_date) as year,
            log_data.as_date as start_time,
            log_data.userId user_id,
            log_data.level as level,
            song_data.song_id song_id,
            song_data.artist_id artist_id,
            log_data.sessionId session_id,
            log_data.location as location,
            log_data.userAgent user_agent,
            row_number() OVER(ORDER BY log_data.sessionId ) as songplay_id
        FROM log_data 
        LEFT JOIN song_data 
        ON (log_data.artist=song_data.artist_name AND log_data.song=song_data.title)
        WHERE log_data.page='NextSong' """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode('overwrite').parquet(output_data+"songPlays.parquet")
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://timshin-test/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    
if __name__ == "__main__":
    main()