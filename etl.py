import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df['song_id', 'title', 'artist_id', 'year', 'duration']

    # Dropping duplicates from the songs_table
    songs_table = songs_table.drop_duplicates(subset=['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = song_df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']

    # Dropping duplicates from the artists_table
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    log_df = spark.read.json(log_data)

    # Filtering by 'Next Song' action
    log_df = log_df.filter(log_df['page'] == 'NextSong')

    # extract columns for users table
    users_table = log_df['userId', 'firstName', 'lastName', 'gender', 'level']

    # Dropping duplicates from the users_table
    users_table = users_table.drop_duplicates(subset=['userId'])

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # Creating a UDF to extract the information needed appropriately
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))

    # Creating new 'start_date' column using UDF defined above
    log_df = log_df.withColumn('start_date', get_datetime(log_df.ts))

    # Adding month to log_df for later use
    log_df = log_df.withColumn('month', month(log_df.start_date))

    # extract columns to create time table
    time_table = log_df.select('start_date',
                           hour('start_date').alias('hour'),
                           dayofmonth('start_date').alias('day'),
                           weekofyear('start_date').alias('week'),
                           month('start_date').alias('month'),
                           year('start_date').alias('year'),
                           dayofweek('start_date').alias('weekday')
                          )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time/')

    # read in song data to use for songplays table
    songs_table_df = spark.read.parquet(output_data + '/songs')
    log_df = log_df.join(songs_table_df, (log_df.song == songs_table_df.title))

    # extract columns from joined song and log datasets to create songplays table
    songplay_table = log_df['start_date', 'userId', 'level', 'song_id', 'artist_id', 'location', 'userAgent', 'year', 'month']
    songplay_table = songplay_table.withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files
    songplay_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songplay/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
