
--CREATING TABLES
CREATE OR REPLACE TABLE SPOTIFY_ALBUM_STG(JSON_DATA VARIANT);

CREATE OR REPLACE TABLE SPOTIFY_ARTIST_STG(JSON_DATA VARIANT);

CREATE OR REPLACE TABLE SPOTIFY_SONG_STG(JSON_DATA VARIANT);


CREATE OR REPLACE TABLE SPOTIFY_ALBUM(
ID NUMBER,
ALBUM_ID STRING,
ALBUM_NAME STRING,
ALBUM_RELEASE_DATE DATE,
ALBUM_TOTAL_TRACKS NUMBER,
ALBUM_URL STRING);


CREATE OR REPLACE TABLE SPOTIFY_ARTIST(
ID NUMBER,
ARTIST_ID STRING,
ARTIST_NAME STRING,
EXTERNAL_URL STRING);


CREATE OR REPLACE TABLE SPOTIFY_SONG(
ID NUMBER,
SONG_ID STRING,
SONG_NAME STRING,
SONG_DURATION NUMBER,
SONG_URL STRING,
SONG_POPULARITY NUMBER,
SONG_ADDED DATE,
ALBUM_ID STRING,
ARTIST_ID STRING);


--FILEFORMAT
CREATE OR REPLACE FILE FORMAT SPOTIFY_FILE_FORMAT
TYPE=CSV
FIELD_DELIMITER=','
RECORD_DELIMITER='\n'
SKIP_HEADER=1
ERROR_ON_COLUMN_COUNT_MISMATCH=false;


-- STAGE

CREATE OR REPLACE STAGE SPOTIFY_STAGE FILE_FORMAT=SPOTIFY_FILE_FORMAT;

--STREAM
CREATE STREAM ALBUM_STREAM ON TABLE SPOTIFY_ALBUM;
CREATE STREAM ARTIST_STREAM ON TABLE SPOTIFY_ARTIST;
CREATE STREAM SONG_STREAM ON TABLE SPOTIFY_SONG;
