import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create a session with Spark.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Process Song dataset and create songs_table and artist_table from it.
    Inputs:
    * spark: spark session instance
    * input data: input file path
    * output_data: output file path
    """
    
    # get filepath to song data file
    song_data = os.path.join( input_data, "song_data/*/*/*/*.json")
    
    # SONG TABLE
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates(['song_id'])
    
    print( "HERE songs_table sample:\n")
    songs_table.show(5)
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')
    
    # ARTISTS TABLE
    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").dropDuplicates(['artist_id'])
    
    print( "HERE artists_table sample:\n")
    artists_table.show(5)
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")

def process_log_data(spark, input_data, output_data):
    """
    Process Log dataset and create users_table, time_table and songplays_table from it.
    Inputs:
    * spark: spark session instance
    * input data: input file path
    * output_data: output file path
    """

    # get filepath to log data file
    log_data = os.path.join( input_data, "log-data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # USERS TABLE
    # extract columns for users table
    users_table = df.select("userId","firstName","lastName","gender","level").dropDuplicates(['userId'])
    
    print( "HERE users_table sample:\n")
    users_table.show(5)
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")

    # TIME TABLE
    # create timestamp column from original timestamp column
    get_start_time = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    get_hour = udf(lambda x: datetime.fromtimestamp(x / 1000.0).hour)
    get_day = udf(lambda x: datetime.fromtimestamp(x / 1000.0).day)
    get_week = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%W'))
    get_month = udf(lambda x: datetime.fromtimestamp(x / 1000.0).month)
    get_year = udf(lambda x: datetime.fromtimestamp(x / 1000.0).year)
    get_weekday = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%A'))

    df = df.withColumn('start_time', get_start_time(df['ts']))
    df = df.withColumn('hour', get_hour(df['ts']))
    df = df.withColumn('day', get_day(df['ts']))
    df = df.withColumn('week', get_week(df['ts']))
    df = df.withColumn('month', get_month(df['ts']))
    df = df.withColumn('year', get_year(df['ts']))
    df = df.withColumn('week_day', get_weekday(df['ts'])).dropDuplicates(['start_time'])

    df.createOrReplaceTempView("time_table")
    
    time_columns = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'week_day']

    # extract columns to create time table
    time_table = spark.sql("""
                        SELECT start_time, hour, day, week, month, year, week_day
                        FROM time_table
                            """).toDF(*time_columns)
    
    print( "HERE time_table sample:\n")
    time_table.show(5)
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])

    # SONGPLAYS TABLE
    # add monotonically increasing id column
    df = df.withColumn('songplay_id', functions.monotonically_increasing_id())
    df.createOrReplaceTempView("songplays_table")

    # song df
    song_data = os.path.join( input_data, "song_data/*/*/*/*.json")
    song_df = spark.read.json(song_data).dropDuplicates()
    song_df.createOrReplaceTempView("songs_table")

    song_columns = ['songplay_id', 'start_time', 'userId', 'level', 'sessionId', 'location', 'userAgent', 'year', 'month',
               'length', 'song_id', 'artist_id', 'title', 'artist_name', 'duration']

    # extract columns to create time table
    songplays_table = spark.sql(
        """
            SELECT sp.songplay_id, sp.start_time, sp.userId, sp.level, sp.sessionId, sp.location, sp.userAgent, sp.year, 
            sp.month, sp.length, s.song_id, s.artist_id, s.title, s.artist_name, s.duration
            FROM songplays_table AS sp 
            JOIN songs_table AS s ON sp.song = s.title AND sp.artist = s.artist_name AND sp.length = s.duration
        """).toDF(*song_columns)
    
    print( "HERE songplays_table sample:\n")
    songplays_table.show(5)
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite", partitionBy=["year","month"])


def main():
    """
    1. Create a Spark Session
    2. Read the Song dataset from S3. Create tables from dataset, write to parquet files.
        Load parquet files to S3.
    3. Read the Log dataset from S3. Create tables from dataset, write to parquet files.
        Load parquet files to S3.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-lake/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
