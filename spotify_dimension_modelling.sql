
select * from tracks;

select * from artists;

ALTER TABLE tracks
ALTER COLUMN played_at TYPE timestamp USING played_at::timestamp;

CREATE TABLE date AS 
SELECT played_at,played_at_date::date FROM tracks;

SELECT * FROM date;

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date as
SELECT
played_at,
played_at_date as date,
TO_CHAR(played_at_date,'YYYYMMDD') AS date_id,
EXTRACT(DAY FROM played_at_date) AS day,
EXTRACT(MONTH FROM played_at_date) AS month,
EXTRACT(YEAR FROM played_at_date) AS year,
EXTRACT(WEEK FROM played_at_date) AS week,
EXTRACT(QUARTER FROM played_at_date) AS quarter,
TO_CHAR(played_at_date, 'Day') AS day_of_week,
CASE 
	WHEN EXTRACT(ISODOW FROM played_at_date) IN (6,7) THEN 'Yes'
	ELSE 'No'
END AS is_weekend
FROM date;


SELECT * FROM dim_date;

SELECT * FROM dim_date 
WHERE day_of_week = 'Friday';

UPDATE dim_date
SET day_of_week = TRIM(day_of_week);

SELECT COUNT(*) FROM (SELECT DISTINCT * FROM dim_date);

CREATE TABLE date_dim AS
SELECT DISTINCT * FROM dim_date
ORDER BY played_at;

SELECT * FROM date_dim;

DROP TABLE dim_date;

ALTER TABLE date_dim
RENAME TO dim_date;

SELECT * FROM dim_date;

CREATE TABLE dim_track AS
SELECT track_id,
	track_name, 
	track_url, 
	context_type, 
	playlist_url, 
	played_at
FROM tracks;

SELECT * FROM dim_track;

CREATE TABLE dim_album AS
SELECT album_id,
	album_name,
	album_artist_id,
	album_release_date,
	album_url 
FROM tracks;

SELECT * FROM dim_album;

select * from artists;

CREATE TABLE dim_artists AS
SELECT DISTINCT * FROM (
SELECT a.artist_id,
	a.artist_name,
	t.artist_url
FROM artists AS a
INNER JOIN tracks AS t ON a.track_id = t.track_id);

SELECT * FROM dim_artists;

CREATE TABLE dim_album AS
SELECT DISTINCT * FROM(
SELECT album_id,
	album_name,
	album_artist_id,
	album_artist_name,
	album_release_date,
	album_url
FROM tracks);


SELECT * FROM dim_album;

ALTER TABLE dim_track
RENAME TO dim_tracks;

CREATE TABLE fact_track_history AS
SELECT DISTINCT * FROM (
SELECT t.track_id,
	ar.artist_id,
	al.album_artist_id,
	al.album_id,
	d.date_id,
	og.played_at,
	og.duration_ms,
	og.explicit,
	og.popularity,
	og.track_number,
	og.total_tracks
FROM tracks as og
JOIN dim_tracks as t ON og.track_id = t.track_id
JOIN dim_artists as ar ON og.album_artist_id = ar.artist_id
JOIN dim_album as al ON og.album_id = al.album_id
JOIN dim_date as d ON og.played_at =  d.played_at)
ORDER BY played_at;

select count(*) from fact_track_history;




