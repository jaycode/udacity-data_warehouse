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
        registration varchar,
        sessionId varchar,
        song varchar,
        status varchar,
        ts bigint,
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
        start_time bigint not null,
        user_id int not null,
        level varchar,
        song_id varchar not null,
        artist_id varchar not null,
        session_id int,
        location varchar,
        user_agent text,
        PRIMARY KEY (songplay_id));
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id int,
        first_name varchar,
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
        start_time bigint,
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
    COMPUPDATE OFF
    JSON {};
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role '{}'
    REGION 'us-west-2'
    COMPUPDATE OFF
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
        AND se.session_id NOT IN (
            SELECT DISTINCT s.session_id FROM songplays s WHERE s.user_id = se.user_id
                AND s.start_time = se.start_time AND s.session_id = se.session_id )
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE page = 'NextSong'
    WHERE user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (
        SELECT DISTINCT start_time 
        FROM staging_events
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
