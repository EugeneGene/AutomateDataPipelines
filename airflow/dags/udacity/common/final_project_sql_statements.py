class SqlQueries:
    # CREATE TABLES
    staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS log_data (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId BIGINT);""")

    staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS song_data (
    artist_id   VARCHAR NOT NULL,
    artist_name VARCHAR,
    artist_location VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    song_id VARCHAR NOT NULL,
    title VARCHAR,
    duration FLOAT,
    year INT);""")

    songplay_table_create = ("""CREATE TABLE IF NOT EXISTS factSongPlays (
    songplay_id  BIGINT IDENTITY(1,1) PRIMARY KEY,
    start_time   BIGINT NOT NULL REFERENCES dimTime(start_time),
    user_key     BIGINT NOT NULL REFERENCES dimUsers(user_key),
    user_id      BIGINT,
    level        VARCHAR NOT NULL,
    song_id      VARCHAR NOT NULL REFERENCES dimSongs(song_id),
    artist_id    VARCHAR NOT NULL REFERENCES dimArtists(artist_id),
    session_id   INT NOT NULL,
    location     VARCHAR,
    user_agent   TEXT NOT NULL);""")

    user_table_create = ("""CREATE TABLE IF NOT EXISTS dimUsers (
    user_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    user_id    BIGINT,
    first_name VARCHAR,
    last_name  VARCHAR,
    gender     VARCHAR,
    level      VARCHAR NOT NULL);""")

    song_table_create = ("""CREATE TABLE IF NOT EXISTS dimSongs (
    song_id   VARCHAR NOT NULL PRIMARY KEY,
    title     VARCHAR,
    artist_id VARCHAR NOT NULL REFERENCES dimArtists(artist_id),
    year      INT NOT NULL,
    duration  INT NOT NULL);""")

    artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimArtists (
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name      VARCHAR,
    location  VARCHAR,
    latitude  FLOAT,
    longitude FLOAT);""")

    time_table_create = ("""CREATE TABLE IF NOT EXISTS dimTime (
    start_time BIGINT NOT NULL PRIMARY KEY,
    hour       INT NOT NULL,
    day        INT NOT NULL,
    week       INT NOT NULL,
    month      INT NOT NULL,
    year       INT NOT NULL,
    weekday    INT NOT NULL);""")




    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM log_data
            WHERE page='NextSong') events
            LEFT JOIN song_data songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM song_data
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM song_data
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)