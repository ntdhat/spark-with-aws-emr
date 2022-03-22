### Date created
03/21/2022

## Data Lake for Sparkify

### Description

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project is built for the purpose of creating an ETL pipeline that extracts Sparkify's data from S3, process them using Spark, and loads that data back into S3 as a set of dimension and fact tables dedicated for queries on song play analysis.

### Instructions

1. This project requires the AWS EMR cluster is available and has the read/write access to S3
2. Input needed EMR's specifications as well as S3 data paths into `dwh.cfg`
3. Copy both `etl.py` and `dl.cfg` to the same directory on EMR cluster
4. Establish SSH Tunnel to EMR Cluster's master node
5. Submit `etl.py` to Spark to process data
6. Use Jupyter Notebook on your local machine or EMR Notebook to test sample queries from `test_queries.ipynb`

### Files in the project

1. `dl.cfg` keeps data lake configuarions, S3 bucket links, and other credentials.
2. `etl.py` is the main ETL logic. Using Spark's Dataframe API, it extracts files from S3 into staging tables, transforms, and loads them into dimentional & fact tables. Finally, it writes all dimension and fact tables back to S3.
3. `test_queries.ipynb` provides example queries (using Spark SQL API) from the perspective of Data Analysis showing song play analysis.
4. `dataset_explore.ipynb` explores the pre-processed data on S3 for initial insights.
5. `README.md` (this file)

### Database schema

**Fact Table**

1. songplays - records in log data associated with song plays
    - songplay\_id, start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user\_agent

**Dimension Tables**

2. users - users in the app
    - user\_id, first\_name, last\_name, gender, level
3. songs - songs in music database
    - song\_id, title, artist\_id, year, duration
4. artists - artists in music database
    - artist\_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units
    - start\_time, hour, day, week, month, year, weekday

### The Datasets

There are 2 datasets which reside on S3:

**1. Song dataset** (s3://udacity-dend/song_data)

The first dataset consists of log files in JSON format containing metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json

**2. Log dataset** (s3://udacity-dend/log_data)

The second dataset consists of log files in JSON format. The log files are partitioned by year and month. For example, here are filepaths to two files in this dataset.

    log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json
