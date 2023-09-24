import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist varchar,
        auth text,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        sessionId int,
        song varchar,
        status int,
        ts timestamp,
        userAgent text,
        userId int);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int identity(1,1),
        start_time timestamp not null,
        user_id varchar not null,
        level varchar,
        song_id varchar not null,
        artist_id varchar not null,
        session_id varchar,
        location varchar,
        user_agent text,
        PRIMARY KEY (songplay_id));
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id varchar,
        first_name varchar NOT NULL,
        last_name varchar,
        gender varchar,
        level varchar,
        PRIMARY KEY (user_id));
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id varchar,
        title varchar not null,
        artist_id varchar not null,
        year int,
        duration float,
        PRIMARY KEY (song_id));
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id varchar,
        name varchar not null,
        location varchar,
        latitude float,
        longitude float,
        PRIMARY KEY (artist_id));
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time timestamp,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int,
        PRIMARY KEY (start_time));
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    JSON {};
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role '{}'
    REGION 'us-west-2'
    JSON 'auto';
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time,
                           user_id,
                           level,
                           song_id,
                           artist_id,
                           session_id,
                           location,
                           user_agent)
    SELECT se.ts as start_time,
           se.userId as user_id,
           se.level as level,
           ss.song_id as song_id,
           ss.artist_id as artist_id,
           se.sessionId as session_id,
           se.location as location,
           se.userAgent as user_agent
    FROM staging_events se
        LEFT JOIN staging_songs ss
            ON se.song = ss.title
            AND se.artist = ss.artist_name
            AND se.length = ss.duration
    WHERE se.page = 'NextSong'
        AND se.sessionId NOT IN (
            SELECT DISTINCT s.session_id FROM songplays s WHERE s.user_id = se.userId
                AND s.start_time = se.ts AND s.session_id = se.sessionId )
        AND ss.song_id IS NOT NULL
        AND ss.artist_id IS NOT NULL
        AND se.userId IS NOT NULL
""")

# # The code below returns duplicate users
# user_table_insert = ("""
#     INSERT INTO users (user_id, first_name, last_name, gender, level)
#     SELECT DISTINCT userId,
#             firstName,
#             lastName,
#             gender,
#             level
#     FROM FROM staging_events
#     WHERE page = 'NextSong'
#     AND userId NOT IN (SELECT DISTINCT user_id FROM users)
# """)

# # This works. Suggested in https://knowledge.udacity.com/questions/42129
# user_table_insert = """
# INSERT INTO users (user_id, first_name, last_name, gender, level)
# SELECT userId AS user_id,
#     firstname AS first_name,
#     lastName AS last_name,
#     gender,
#     level
# FROM staging_events m
# WHERE userId IS NOT null AND ts = (select max(ts) FROM staging_events s WHERE s.userId = m.userId)
# ORDER BY userId DESC
# """

# This also works
user_table_insert = """
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    WITH uniq_staging_events AS (
    	SELECT userId, firstName, lastName, gender, level,
    		   ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) AS rank
    	FROM staging_events
                WHERE userId IS NOT NULL AND page = 'NextSong'
    )
    SELECT userId, firstName, lastName, gender, level
    	FROM uniq_staging_events
    WHERE rank = 1
"""

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
           title,
           artist_id,
           year,
           duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        ts,
        EXTRACT(hr from ts) AS hour,
        EXTRACT(d from ts) AS day,
        EXTRACT(w from ts) AS week,
        EXTRACT(mon from ts) AS month,
        EXTRACT(yr from ts) AS year,
        EXTRACT(weekday from ts) AS weekday
    FROM (
        SELECT DISTINCT ts
        FROM staging_events
    )
    WHERE ts NOT IN (SELECT DISTINCT start_time FROM time)
        AND ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
