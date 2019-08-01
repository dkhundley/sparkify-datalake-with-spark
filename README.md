# Sparkify - Data Lake with Spark

With data ever increasing in size and complexity, it is very common for companies these days to be leveraging the concept of a data lake and data lake technology. Continuing on with our fictional Spotify-like company, Sparkify, we will be establishing data lake principles of our own by leveraging Spark.

Specifically, we will be pulling song data and log data from an S3 bucket provided by Udacity and moving the information into appropriate fact/dimension tables stored as parquet files.

## Data Schema
Here is the schema that the data is being transformed into:

### Fact Table

- **songplays** - records in log data associated with song plays i.e. records with page NextSong
  - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### Dimension Tables

- **users** - users in the app
  - *user_id, first_name, last_name, gender, level*
- **songs** - songs in music database
  - *song_id, title, artist_id, year, duration*
- **artists** - artists in music database
  - *artist_id, name, location, latitude, longitude*
- **time** - timestamps of records in songplays broken down into specific units
  - *start_time, hour, day, week, month, year, weekday*

## Files Included
- **etl.ipynb**: This Jupyter notebook contains all the work that I completed to test out that each individual piece of code worked prior to moving it into etl.py.
- **dl.cfg**: This configuration file contained my AWS credentials in order to be able to properly access AWS information. Due to the sensitive nature of that information, it has been wiped prior to uploading the information into GitHub.
- **etl.py**: This contains the hardened code for extraction, transformation, and load (ETL) from the Jupyter notebook of the same name mentioned above.
