# udacity-data_warehouse
Submission for Udacity's Data Warehouse project.

- [Link to rubric](https://review.udacity.com/#!/rubrics/2501/view)
- [Link to project instructions](https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/58ff61b9-a54f-496d-b4c7-fa22750f6c76/lessons/b3ce1791-9545-4187-b1fc-1e29cc81f2b0/concepts/last-viewed?contentVersion=2.0.0&contentLocale=en-us)

**Note:** Before running the scripts, rename `dwh.default.cfg` to `dwh.cfg` and update the config parameters as needed.

## Introduction
The purpose of the database is to make it easier for the data science team at Sparkify to analyze their app's users' behaviors. Searching for particular song play event or a group of them can be done by running SQL queries to the final database.

## Schema Design

![schema](schema.png)

List of tables
- staging_events: Staging table for data from `s3://udacity-dend/log_data`
- staging_songs: Staging table for data from `s3://udacity-dend/song_data`
- songplays: Log data of song plays.
- users: Users in the app.
- songs: Songs in the app's music database.
- artists: Artists in the app's music database.
- time: Timestamps of records in songplays.

## ETL Pipeline

1. Delete if exists and create all tables listed in the "Schema Design" section above.
2. Copy all data from `s3://udacity-dend/song_data` and `s3://udacity-dend/log_data` to staging tables `staging_songs` and `staging_events`, respectively. These staging tables have the exact same structure with the raw json files.
3. Run SQL queries to select data from staging tables and then directly insert them to the other 5 tables (i.e. our OLAP/analytical tables). When running the insert code, handle duplicate records by not inserting values when duplicates are found (code like `WHERE user_id NOT IN (SELECT DISTINCT user_id FROM users)` deals with this problem). 

## Additional

I also created a `check_files.py` script to check if the files in Udacity's S3 instance existed. To run this code, `ACCESS_KEY` and `ACCESS_SECRET` need to also be set in the config file `dwh.cfg`.