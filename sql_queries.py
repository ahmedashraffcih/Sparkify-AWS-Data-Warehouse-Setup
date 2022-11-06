import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays cascade"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

# artist, length and song may be null as some clicks on the website
# refer to page visits of e.g. the "upgrade account" page, so there is 
# no corresponding song

staging_events_table_create= ("""
CREATE TABLE staging_events(
	artist 			varchar(250),
	auth 			varchar(20) 	not null,
	firstName		varchar(15),
	gender			char(1),
	itemInSession 	integer			not null,
	lastName		varchar(15),
	length			real,
	level			varchar(4)		not null,
	location		varchar(150),
	method			varchar(4)		not null,
	page			varchar(50)		not null,
	registration	real,
	sessionId		integer			not null,
	song			varchar(400),
	status			smallint		not null,
	ts				bigint			not null,
	userAgent		varchar(400),
	userId			varchar(18)		not null
)""")


staging_songs_table_create = ("""
CREATE TABLE staging_songs(
	artist_id			varchar(18)		not null,
	artist_location		varchar(300),
	artist_latitude		real,
	artist_longitude	real,
	artist_name 		varchar(250) 	not null,
	duration			real			not null,
	num_songs			int 			not null,
	song_id				varchar(18)		not null,
	title				varchar(400)	not null,
	year				smallint		not null
)""")




songplay_table_create = ("""
CREATE TABLE songplays(
	songplay_id		integer			IDENTITY(0,1) 	primary key,
	start_time		timestamp		not null 	sortkey references times (start_time),
	user_id			varchar(18)		not null 	references users (user_id),
	song_id			varchar(18) 	not null 	distkey references songs (song_id),
	artist_id		varchar(18) 	not null 	references artists (artist_id),
	session_id		integer			not null,
	location		varchar(150),
	user_agent		varchar(400)
)""")


user_table_create = ("""
CREATE TABLE users (
	user_id			varchar(18)		primary key,
	first_name		varchar(15),
	last_name		varchar(15),
	gender			char(1),
	level			varchar(4)		not null
)
diststyle all;
""")


song_table_create = ("""
CREATE TABLE songs (
	song_id			varchar(18)		primary key	distkey,
	title			varchar(400)	not null,
	artist_id		varchar(18)		not null,
	year			smallint		not null,
	duration		real			not null
)
""")

artist_table_create = ("""
CREATE TABLE artists (
	artist_id		varchar(18)		primary key,
	name			varchar(250)	not null,
	location		varchar(300),
	latitude		real,
	longitude		real
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE times (
	start_time		timestamp		primary key	sortkey,
	hour			smallint		not null,
	day				smallint		not null,
	week			smallint		not null,
	month			smallint		not null,
	year			smallint		not null,
	weekday			boolean			not null
)
diststyle auto;
""")


# STAGING TABLES
staging_events_copy = (""" 
    copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get("IAM_ROLE", "ARN"))

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto';
""").format(config.get("IAM_ROLE", "ARN"))



# FINAL TABLES

time_table_insert = ("""
INSERT INTO times (start_time, hour, day, week, month, year, weekday)
SELECT
    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
    EXTRACT(hour FROM start_time) as hour,
    EXTRACT(day FROM start_time) as day,
    EXTRACT(week FROM start_time) as week,
    EXTRACT(month FROM start_time) as month,
    EXTRACT(year FROM start_time) as year,
    CASE WHEN EXTRACT(dayofweek FROM start_time) IN (1,2,3,4,5) THEN true ELSE false END as weekday
FROM staging_events
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT 
	userId as user_id,
	firstName as first_name,
	lastName as last_name,
	gender,
	level
FROM staging_events
""")


song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
	song_id,
	title,
	artist_id,
	year,
	duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
	artist_id,
	artist_name as name,
	artist_location as location,
	artist_latitude as latitude,
	artist_longitude as longitude
FROM staging_songs
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id, location, user_agent)
SELECT
	TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time,
	e.userId as user_id,
	s.song_id as song_id,
	s.artist_id as artist_id,
	e.sessionId as session_id,
	e.location as location,
	e.userAgent as user_agent
FROM staging_events e 
JOIN staging_songs s ON (e.song = s.title)
""")

# BUGFIXING

get_load_errors = "SELECT * FROM stl_load_errors ORDER BY starttime"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]