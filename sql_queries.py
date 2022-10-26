# DROP TABLES
songplay_table_drop = (""" 
DROP table IF EXISTS songplay_table  
""")
  
user_table_drop = (""" 
DROP table IF EXISTS user_table
""")

song_table_drop = (""" 
DROP table IF EXISTS song_table
""")

artist_table_drop = (""" 
DROP table IF EXISTS artist_table
""")
    
time_table_drop = (""" 
DROP table IF EXISTS time_table 
""")

# CREATE TABLES

user_table_create = (""" 
CREATE TABLE IF NOT EXISTS user_table (
    user_id int NOT NULL,
    first_name text NOT NULL, 
    last_name text NOT NULL, 
    gender varchar NOT NULL, 
    level text, 
    PRIMARY KEY (user_id));
""")

song_table_create = (""" 
CREATE TABLE IF NOT EXISTS song_table (
    song_id varchar, 
    title text NOT NULL, 
    artist_id varchar NOT NULL, 
    year int NOT NULL, 
    duration float NOT NULL, 
    PRIMARY KEY (song_id));
""")

artist_table_create = (""" 
CREATE TABLE IF NOT EXISTS artist_table (
    artist_id varchar, 
    artist_name varchar NOT NULL, 
    artist_location text, 
    artist_latitude float NOT NULL, 
    artist_longitude float NOT NULL, 
    PRIMARY KEY (artist_id));
""")

time_table_create = (""" 
CREATE TABLE IF NOT EXISTS time_table (
    start_time timestamp NOT NULL, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday varchar NOT NULL, 
    PRIMARY KEY (start_time));
""")

songplay_table_create = (""" 
CREATE TABLE IF NOT EXISTS songplay_table (
    songplay_id SERIAL,
    start_time timestamp, 
    user_id int, 
    level text, 
    song_id varchar, 
    artist_id varchar, 
    session_id int NOT NULL, 
    artist_location text NOT NULL, 
    user_agent text NOT NULL,
    PRIMARY KEY (songplay_id));
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplay_table (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    artist_location, 
    user_agent, 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO user_table (
    user_id,
    first_name, 
    last_name, 
    gender, 
    level)
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO song_table (
    song_id,
    title,
    artist_id,
    year,
    duration)
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artist_table (
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude) 
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time_table (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT artist_table.artist_id, song_table.song_id
FROM song_table 
JOIN artist_table ON artist_table.artist_id = song_table.artist_id
WHERE song_table.title = (%s) AND artist_table.artist_name = (%s) AND song_table.duration = (%s);
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
