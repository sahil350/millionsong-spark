import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    This procedure creates a SparkSession object setting the jars to interact with files on AWS
    
    RETURNS:
    * SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This procedure reads the song data and then creates songs and artist table 
    from it.
    
    INPUTS:
    * spark the SparkSession object
    * input_data the path of songs data files
    * ouput_data the s3 path to save resulting parquet files
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create a temporary view of songs data
    df.createOrReplaceTempView('songs')

    # extract columns to create songs table
    songs_table = spark.sql('''
                    SELECT DISTINCT song_id,
                           title,
                           artist_id,
                           year,
                           duration
                    FROM songs
                    WHERE song_id IS NOT NULL AND 
                          artist_id IS NOT NULL
                    ''')
    
    # songs table write path
    songs_table_path = output_data + 'songs'

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(songs_table_path)

    # extract columns to create artists table
    artists_table = spark.sql('''
                       SELECT DISTINCT artist_id, 
                              artist_name AS name,
                              artist_location AS location,
                              artist_latitude AS latitude, 
                              artist_longitude AS longitude
                       FROM SONGS
                       WHERE artist_id IS NOT NULL
                    ''')
    
    # artists table write path
    artist_table_path = output_data + 'artists'

    # write artists table to parquet files
    artists_table.write.parquet(artist_table_path)


def process_log_data(spark, input_data, output_data):
    """
    This procedure reads the log(events) data and then creates songplays, users 
    and time table from it.
    
    INPUTS:
    * spark the SparkSession object
    * input_data the path of songs data files
    * ouput_data the s3 path to save resulting parquet files
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(logs_path)
    
    # filter by actions for song plays
    df = df.filter(df_log.page=='NextSong')
    
    # create a temporary view of events
    df.createOrReplaceTempView('events')

    # extract columns for users table    
    users_table = spark.sql('''
            SELECT DISTINCT CAST(userId AS INT) AS user_id,
                            firstName AS first_name,
                            lastName AS last_name,
                            gender,
                            level
            FROM events
            WHERE userId IS NOT NULL
            ''')
    
    # users table write path
    users_table_path = output_data + "users"
    
    # write users table to parquet files
    users_table.write.parquet(users_path)

    # get timestamp
    ts = spark.sql('''
                SELECT to_timestamp(ts/1000) AS start_time
                FROM events
            ''')

    # create a temporary view of timestamp
    ts.createOrReplaceTempView('times')
    
    # extract columns to create time table
    time_table = spark.sql('''
                SELECT start_time,
                       hour(start_time) AS hour,
                       day(start_time) AS day,
                       weekofyear(start_time) AS week,
                       month(start_time) AS month,
                       year(start_time) AS year,
                       weekday(start_time) AS weekday
                FROM times 
            ''')
    
    # time table write path
    time_path = output_data + 'time'
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(time_path)

    # read in song data to use for songplays table
    # song parquet path
    songs_dir = output + 'songs'
    song_df = spark.read.parquet(songs_dir)
    
    # create a temporary view of songs table
    songs.createOrReplaceTempView('songs_data')
    
    # read in artist data to use for songplays table
    # artist parquet path
    artists_dir = output + 'artists'
    artist_df = spark.read.parquet(atists_dir)
    
    #create a temporary view of artists table
    artists.createOrReplaceTempView('artists')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                        SELECT to_timestamp(events.ts/1000) AS start_time,
                               events.userId AS user_id,
                               events.level,
                               songs_data.song_id,
                               artists.artist_id,
                               events.sessionId AS session_id,
                               artists.location,
                               events.userAgent AS user_agent
                        FROM artists 
                        JOIN songs_data ON artists.artist_id = songs_data.artist_id
                        JOIN events ON artists.name = events.artist
                    ''')
    
    # create a temporary view of songplays table for partition by year and month
    songplays.createOrReplaceTempView('songplays')
    
    songplays = spark.sql('''
            SELECT *, year(start_time) AS year,
                    month(start_time) AS month
            FROM songplays
            ''')

    # songplays write path
    songplays_path = output_data + 'songplays'
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(songplays_path)


def main():
    """
    This procedure creates a SparkSession object, sets input and output paths,
    calls the procedure to process songs and log (events) data, and finally 
    closes the SparkSession object
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://millionsong-project/analytics/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.close()


if __name__ == "__main__":
    main()
