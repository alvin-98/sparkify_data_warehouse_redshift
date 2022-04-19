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
CREATE TABLE IF NOT EXISTS staging_events
(
artist TEXT NOT NULL,
auth VARCHAR(20) NOT NULL,
firstName TEXT NOT NULL,
gender CHAR,
itemInSession BOOLEAN,
lastName TEXT,
length FLOAT,
level VARCHAR(10),
location TEXT,
method VARCHAR(5),
page VARCHAR(10),
registration TEXT,
sessionId TEXT,
song TEXT,
status SMALLINT,
ts TIMESTAMP,
userAgent TEXT,
userId TEXT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs INTEGER,
artist_id TEXT NOT NULL,
artist_latitude TEXT,
artist_longitude TEXT,
artist_location TEXT,
artist_name TEXT NOT NULL,
song_id TEXT PRIMARY KEY,
title TEXT,
duration FLOAT,
year SMALLINT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP,
user_id TEXT,
level VARCHAR(10),
song_id TEXT,
artist_id TEXT,
session_id TEXT,
location TEXT,
user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id TEXT PRIMARY KEY,
first_name TEXT,
last_name TEXT,
gender BOOLEAN,
level VARCHAR(10)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id TEXT PRIMARY KEY,
title TEXT,
artist_id TEXT,
year SMALLINT,
duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id TEXT PRIMARY KEY,
name TEXT,
location TEXT,
latitude TEXT,
longitude TEXT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP PRIMARY KEY,
hour SMALLINT,
day SMALLINT,
week SMALLINT,
month SMALLINT,
year SMALLINT,
weekday SMALLINT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {} 
credentials 'aws_iam_role={}'
json {}
region 'us-east-1';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs
FROM {}
credentials 'aws_iam_role={}'
json 'auto'
region 'us-east-1';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time,
user_id,
level,
song_id,
artist_id,
session_id,
location,
user_agent)
SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' as start_time, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent 
FROM staging_events as se JOIN staging_songs as ss ON (se.song == ss.title) AND (se.artist == ss.artist_name)
WHERE se.page = 'NextSong' 
""")

user_table_insert = ("""
INSERT INTO users
(user_id,
first_name,
last_name,
gender,
level
)
SELECT distinct userId, firstName, lastName, gender, level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs
(song_id,
title,
artist_id,
year,
duration)
SELECT distinct song_id, title, artist_id, year, duration
FROM stating_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id,
name,
location,
latitude,
longitude)
SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekDay)
SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time) 
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
