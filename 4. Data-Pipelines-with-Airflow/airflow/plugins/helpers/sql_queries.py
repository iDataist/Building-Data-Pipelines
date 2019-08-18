class SqlQueries:
    
    songplay_table_insert = ("""
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
    SELECT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time,  \
    e.userId AS user_id,  \
    e.level,  \
    s.song_id,  \
    s.artist_id,  \
    e.sessionId AS session_id,  \
    s.artist_location AS location,  \
    e.userAgent AS user_agent \
    FROM staging_songs s \
    RIGHT JOIN staging_events e \
    ON s.title = e.song \
    AND s.artist_name = e.artist \
    AND s.duration = e.length \
    WHERE page = 'NextSong';""")

    user_table_insert = ("""
    (user_id, first_name, last_name, gender, level) \
    SELECT DISTINCT userId AS user_id,  \
    firstName AS first_name,  \
    lastName AS last_name,  \
    gender,  \
    level \
    FROM staging_events
    WHERE page = 'NextSong';""")

    song_table_insert = ("""
    (song_id, title, artist_id, year, duration) \
    SELECT DISTINCT song_id,  \
    title,  \
    artist_id,  \
    year,  \
    duration \
    FROM staging_songs;""")

    artist_table_insert = ("""
    (artist_id, name, location, lattitude, longitude) \
    SELECT DISTINCT artist_id,  \
    artist_name AS name,  \
    artist_location AS location,  \
    artist_latitude AS lattitude,  \
    artist_longitude AS longitude \
    FROM staging_songs;""")

    time_table_insert = ("""
    (start_time, hour, day, week, month, year, weekday) \
    SELECT DISTINCT start_time, \
    EXTRACT (hour from start_time) AS hour, \
    EXTRACT (day from start_time)AS day, \
    EXTRACT (week from start_time) AS week, \
    EXTRACT (month from start_time) AS month, \
    EXTRACT (year from start_time) AS year, \
    EXTRACT (dayofweek from start_time) AS weekday \
    FROM songplays;""")    
