# Sparkify Data Warehouse

This project is a data warehousing solution for Sparkify, a music streaming service. It uses Apache Spark to extract data from JSON logs and stores it in a star schema data model in Amazon Redshift. 

The star schema design includes one fact table, songplays, and four dimension tables, users, songs, artists, and time. The fact table contains information about the songs that users listened to, while the dimension tables contain information about users, songs, artists, and timestamps.

## How to Use

To use this project, follow the steps below:

1. Clone the repository to your local machine.
2. Create an Amazon Redshift cluster and update the `dwh.cfg` file with the appropriate credentials and cluster details.
3. Run `python create_tables.py` to create the tables in the database.
4. Run `python etl.py` to extract the data from the JSON logs and load it into the tables in the database.

## Files in this Repository

- `create_tables.py`: This script creates the fact and dimension tables for the star schema in Amazon Redshift.
- `dwh.cfg`: This configuration file contains the credentials and cluster details for the Amazon Redshift cluster.
- `etl.py`: This script loads data from S3 into staging tables on Redshift and then processes that data into the fact and dimension tables.
- `README.md`: This file which is a documentaion for the project.
- `sql_queries.py`: This file contains the SQL statements used in `create_tables.py` and `etl.py`.


## Database Schema

### Fact Table
- `songplays` - records in event data associated with song plays. Fields for the table include:
    - `songplay_id`: Unique identifier for each song play.
    - `start_time`: Timestamp of when the song was played.
    - `user_id`: Unique identifier for the user who played the song.
    - `level`: The subscription level of the user.
    - `song_id`: Unique identifier for the song that was played.
    - `artist_id`: Unique identifier for the artist of the song that was played.
    - `session_id`: Unique identifier for the user's session.
    - `location`: The location where the song was played.
    - `user_agent`: The user agent of the user's browser.

### Dimension Tables
- `users` - users in the app. Fields for the table include:
    - `user_id`: Unique identifier for each user.
    - `first_name`: First name of the user.
    - `last_name`: Last name of the user.
    - `gender`: The gender of the user.
    - `level`: The subscription level of the user.

- `songs` - songs in music database. Fields for the table include:
    - `song_id`: Unique identifier for each song.
    - `title`: The title of the song.
    - `artist_id`: Unique identifier for the artist of the song.
    - `year`: The year the song was released.
    - `duration`: The duration of the song.

- `artists` - artists in music database. Fields for the table include:
    - `artist_id`: Unique identifier for each artist.
    - `name`: The name of the artist.
    - `location`: The location of the artist.
    - `latitude`: The latitude of the artist's location.
    - `longitude`: The longitude of the artist's location.

- `time` - timestamps of records in songplays broken down into specific units. Fields for the table include:
  - `start_time`: Timestamp of when the song was played.
  - `hour`: The hour of the start_time.
  - `day`: The day of the start_time.
  - `week`: The week of the start_time.
  - `month`: The month of the start_time.
  - `year`: The year of the start_time.
  - `weekday`: The day of the week of the start_time.

## Credits
This project was completed as part of the Udacity Data Engineering Nanodegree program. The template code and starter files were provided by Udacity, while the data warehousing solution was developed by me, Marc Sanad.
