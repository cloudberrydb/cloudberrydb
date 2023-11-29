
-- turn on vectorization

SET vector.enable_vectorization=on;
SET vector.max_batch_size = 128;

-- query 31-40

SELECT SearchEngineID, ClientIP, count() AS c, sum(Refresh) FROM hits WHERE SearchPhrase != '' GROUP BY SearchEngineID, ClientIP ORDER BY SearchEngineID, ClientIP, c LIMIT 10;

SELECT WatchID, ClientIP, count() AS c, sum(Refresh) FROM hits WHERE SearchPhrase != '' GROUP BY WatchID, ClientIP ORDER BY WatchID, ClientIP, c LIMIT 10;

SELECT WatchID, ClientIP, count() AS c, sum(Refresh) FROM hits GROUP BY WatchID, ClientIP ORDER BY WatchID, ClientIP, c LIMIT 10;

SELECT URL, count() AS c FROM hits GROUP BY URL ORDER BY url, c LIMIT 10;

SELECT 1, URL, count() AS c FROM hits GROUP BY 1, URL ORDER BY url, c LIMIT 10;

SELECT ClientIP AS x, ClientIP - 1 AS y, ClientIP - 2 AS z, ClientIP - 3 AS w, count() AS c FROM hits GROUP BY x, y, z, w ORDER BY x, y, z, w, c LIMIT 10;

SELECT URL, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT DontCountHits = 1::smallint AND NOT Refresh = 1::smallint AND URL != '' GROUP BY URL ORDER BY PageViews LIMIT 10;

SELECT Title, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT DontCountHits = 1::smallint AND NOT Refresh = 1::smallint AND Title != '' GROUP BY Title ORDER BY PageViews LIMIT 10;

SELECT URL, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT Refresh = 1::smallint AND IsLink = 1::smallint AND NOT IsDownload = 1::smallint GROUP BY URL ORDER BY PageViews LIMIT 1000;

SELECT TraficSourceID, SearchEngineID, AdvEngineID, URL AS Dst, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT Refresh = 1::smallint GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Dst ORDER BY PageViews LIMIT 1000;


