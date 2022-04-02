from pyparsing import col
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T, Window
import configparser

def create_spark_session(appName: str) -> SparkSession:
    spark = SparkSession\
        .builder\
        .appName(appName)\
        .getOrCreate()
    return spark

def stageSongsData(spark: SparkSession, song_data_path: str) -> DataFrame:
    """
    Extract the songs data and return a DataFrame of songs data
    
    Args:
        (SparkSession)  spark           - the SparkSession object
        (str)           song_data_path  - path to the songs data on S3
    """
    song_schema = T.StructType([
        T.StructField('artist_id', T.StringType()),
        T.StructField('artist_latitude', T.DoubleType()),
        T.StructField('artist_location', T.StringType()),
        T.StructField('artist_longitude', T.DoubleType()),
        T.StructField('artist_name', T.StringType()),
        T.StructField('duration', T.DoubleType()),
        T.StructField('num_songs', T.IntegerType()),
        T.StructField('song_id', T.StringType()),
        T.StructField('title', T.StringType()),
        T.StructField('year', T.IntegerType())
    ])
    df = spark.read.options(header=True, recursiveFileLookup=True).json(song_data_path, schema=song_schema)
    return df

def stageLogData(spark: SparkSession, log_data_path: str) -> DataFrame:
    """
    Extract the log data and return a DataFrame of log data
    
    Args:
        (SparkSession)  spark           - the SparkSession object
        (str)           log_data_path   - path to the log data on S3
    """
    log_schema = T.StructType([
        T.StructField('artist', T.StringType()),
        T.StructField('auth', T.StringType()),
        T.StructField('firstName', T.StringType()),
        T.StructField('gender', T.StringType()),
        T.StructField('itemInSession', T.IntegerType()),
        T.StructField('lastName', T.StringType()),
        T.StructField('length', T.FloatType()),
        T.StructField('level', T.StringType()),
        T.StructField('location', T.StringType()),
        T.StructField('method', T.StringType()),
        T.StructField('page', T.StringType()),
        T.StructField('registration', T.DoubleType()),
        T.StructField('sessionId', T.StringType()),
        T.StructField('song', T.StringType()),
        T.StructField('status', T.IntegerType()),
        T.StructField('ts', T.LongType()),
        T.StructField('userAgent', T.StringType()),
        T.StructField('userId', T.StringType())
    ])
    df = spark.read.options(header=True, recursiveFileLookup=True).json(log_data_path, schema=log_schema)
    return df

def processUsersData(log_data: DataFrame) -> DataFrame:
    win_spec = Window.partitionBy('userId').orderBy(F.desc('ts'))
    users = log_data\
        .filter((log_data.userId.isNotNull()) & (log_data.userId != ""))\
        .select(
            log_data.userId.alias('user_id'),
            log_data.firstName.alias('first_name'),
            log_data.lastName.alias('last_name'),
            log_data.gender.alias('gender'),
            log_data.level.alias('level'),
            F.row_number().over(win_spec).alias('ts_order')
        )\
        .filter('ts_order==1')\
        .drop('ts_order')
    return users

def processTimeData(log_data: DataFrame) -> DataFrame:
    time = log_data\
        .distinct()\
        .select(
            F.from_unixtime(F.col('ts') / 1000).alias('start_time'),
            F.hour(F.from_unixtime(F.col('ts') / 1000)).alias('hour'),
            F.dayofmonth(F.from_unixtime(F.col('ts') / 1000)).alias('day'),
            F.weekofyear(F.from_unixtime(F.col('ts') / 1000)).alias('week'),
            F.month(F.from_unixtime(F.col('ts') / 1000)).alias('month'),
            F.year(F.from_unixtime(F.col('ts') / 1000)).alias('year'),
            F.dayofweek(F.from_unixtime(F.col('ts') / 1000)).alias('weekday')
        )
    return time

def processSongPlaysData(log_data: DataFrame, songs_data: DataFrame) -> DataFrame:
    join_cond = [log_data.artist == songs_data.artist_name, log_data.song == songs_data.title]
    songplays = log_data.where(log_data.page == 'NextSong').join(songs_data, join_cond)\
        .select(
            F.from_unixtime(log_data.ts / 1000).alias('start_time'),
            log_data.userId.alias('user_id'),
            log_data.level.alias('level'),
            songs_data.song_id.alias('song_id'),
            songs_data.artist_id.alias('artist_id'),
            log_data.sessionId.alias('session_id'),
            log_data.location.alias('location'),
            log_data.userAgent.alias('user_agent'),
            F.month(F.from_unixtime(log_data.ts / 1000)).alias('month'),
            F.year(F.from_unixtime(log_data.ts / 1000)).alias('year')
        )\
        .withColumn('songplay_id', F.row_number().over(Window.orderBy('start_time')))
    return songplays
    
def processSongsData(songs_data: DataFrame) -> DataFrame:
    songs = songs_data\
        .filter((songs_data.song_id.isNotNull()) & (songs_data.song_id != ""))\
        .select(
            F.col('song_id'),
            F.col('title'),
            F.col('artist_id'),
            F.col('year'),
            F.col('duration')
        )\
        .dropDuplicates()
    return songs
    
def processArtistsData(songs_data: DataFrame) -> DataFrame:
    artists = songs_data\
        .filter((songs_data.artist_id.isNotNull()) & (songs_data.artist_id != ""))\
        .select(
            F.col('artist_id'),
            F.col('artist_name').alias('name'),
            F.col('artist_location').alias('location'),
            F.col('artist_latitude').alias('latitude'),
            F.col('artist_longitude').alias('longitude')
        )\
        .dropDuplicates()
    return artists


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
    SPARK_APP_NAME = config.get('EMR', 'EMR_SPARK_APP_NAME')

    LOG_DATA = config.get('S3', 'LOG_DATA')
    SONGS_DATA = config.get('S3', 'SONGS_DATA')

    PROCESSED_DATA_PATH = config.get('S3', 'PROCESSED_DATA_PATH')

    spark = create_spark_session(SPARK_APP_NAME)
    
    # Extract data from S3
    df_songs_staging = stageSongsData(spark, SONGS_DATA)
    df_log_staging = stageLogData(spark, LOG_DATA)

    # Process data extracted
    songs = processSongsData(df_songs_staging)
    artists = processArtistsData(df_songs_staging)
    time = processTimeData(df_log_staging)
    users = processUsersData(df_log_staging)
    songplays = processSongPlaysData(df_log_staging, df_songs_staging)
    
    # Write DataFrame to S3
    artists.write.option('header', True).parquet(PROCESSED_DATA_PATH + 'artists', mode='overwrite')
    users.write.option('header', True).parquet(PROCESSED_DATA_PATH + 'users', mode='overwrite')

    songs.repartition('year', 'artist_id')
    songs.write.partitionBy('year', 'artist_id').option('header', True).parquet(PROCESSED_DATA_PATH + 'songs', mode='overwrite')

    time.repartition('year', 'month')
    time.write.partitionBy('year', 'month').option('header', True).parquet(PROCESSED_DATA_PATH + 'time', mode='overwrite')

    songplays.repartition('year', 'month')
    songplays.write.partitionBy('year', 'month').option('header', True).parquet(PROCESSED_DATA_PATH + 'songplays', mode='overwrite')

    spark.stop()


if __name__ == "__main__":
    main()
