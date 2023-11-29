\timing

-- open vectorization

SET vector.enable_vectorization=on;
SET vector.max_batch_size = 128;

-- q1
EXPLAIN verbose SELECT count() FROM hits;

-- q2
EXPLAIN verbose SELECT count() FROM hits WHERE AdvEngineID != 0::smallint;

-- q3
EXPLAIN verbose SELECT sum(AdvEngineID), count() FROM hits;

-- q4
EXPLAIN verbose SELECT sum(UserID) FROM hits;

-- q5
EXPLAIN verbose SELECT count(distinct UserID) FROM hits;

-- q6
EXPLAIN verbose SELECT count(distinct SearchPhrase) FROM hits;

-- q7
EXPLAIN verbose SELECT min(EventDate), max(EventDate) FROM hits;

-- q8
EXPLAIN verbose SELECT AdvEngineID, count() FROM hits WHERE AdvEngineID != 0::smallint GROUP BY AdvEngineID ORDER BY count();

-- q9
EXPLAIN verbose SELECT RegionID, count(distinct UserID) AS u FROM hits GROUP BY RegionID ORDER BY regionid, u LIMIT 10;

-- q10
EXPLAIN verbose SELECT RegionID, sum(AdvEngineID), count() AS c, count(distinct UserID) FROM hits GROUP BY RegionID ORDER BY regionid, c LIMIT 10;

-- q11
EXPLAIN verbose SELECT MobilePhoneModel, count(distinct UserID) AS u FROM hits WHERE MobilePhoneModel != '' GROUP BY MobilePhoneModel ORDER BY MobilePhoneModel, u LIMIT 10;

-- q12
EXPLAIN verbose SELECT MobilePhone, MobilePhoneModel, count(distinct UserID) AS u FROM hits WHERE MobilePhoneModel != '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY MobilePhone, MobilePhoneModel, u LIMIT 10;

-- q13
EXPLAIN verbose SELECT SearchPhrase, count() AS c FROM hits WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY SearchPhrase, c LIMIT 10;

-- q14
EXPLAIN verbose SELECT SearchPhrase, count(distinct UserID) AS u FROM hits WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY SearchPhrase, u LIMIT 10;

-- q15
EXPLAIN verbose SELECT SearchEngineID, SearchPhrase, count() AS c FROM hits WHERE SearchPhrase != '' GROUP BY SearchEngineID, SearchPhrase ORDER BY SearchEngineID, SearchPhrase, c LIMIT 10;

-- q16
EXPLAIN verbose SELECT UserID, count() FROM hits GROUP BY UserID ORDER BY userid, count() LIMIT 10;

-- q17
EXPLAIN verbose SELECT UserID, SearchPhrase, count() FROM hits GROUP BY UserID, SearchPhrase ORDER BY UserID, SearchPhrase, count() LIMIT 10;

-- q18
EXPLAIN verbose SELECT UserID, SearchPhrase, count() FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;

-- q19
EXPLAIN verbose SELECT UserID, EventTime AS m, SearchPhrase, count() FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY UserID, m, SearchPhrase, count() LIMIT 10;

-- q20
EXPLAIN verbose SELECT UserID FROM hits WHERE UserID = 1234567890123456789;

-- q21
EXPLAIN verbose SELECT count() FROM hits WHERE URL LIKE '%metrika%';

-- q22
EXPLAIN verbose SELECT SearchPhrase, min(URL), count() AS c FROM hits WHERE URL LIKE '%metrika%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY SearchPhrase, c LIMIT 10;

-- q23
EXPLAIN verbose SELECT SearchPhrase, min(URL), min(Title), count() AS c, count(distinct UserID) FROM hits WHERE Title LIKE '%abc%' AND URL NOT LIKE '%.yandex.%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY SearchPhrase, c LIMIT 10;

-- q24
EXPLAIN verbose SELECT URL FROM hits WHERE URL LIKE '%metrika%' ORDER BY EventTime LIMIT 10;

-- q25
EXPLAIN verbose SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY EventTime LIMIT 10;

-- q26
EXPLAIN verbose SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY SearchPhrase LIMIT 10;

-- q27
EXPLAIN verbose SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY EventTime, SearchPhrase LIMIT 10;

-- q28
EXPLAIN verbose SELECT CounterID, count() AS c FROM hits WHERE URL != '' GROUP BY CounterID HAVING count() > 100000 ORDER BY counterid, c LIMIT 25;

-- q29
EXPLAIN verbose SELECT Referer AS key, count() AS c, min(Referer) FROM hits WHERE Referer != '' GROUP BY key ORDER BY key, c LIMIT 25;

-- q30
EXPLAIN verbose SELECT sum(ResolutionWidth), sum(ResolutionWidth + 1), sum(ResolutionWidth + 2), sum(ResolutionWidth + 88), sum(ResolutionWidth + 89) FROM hits;

-- q31
EXPLAIN verbose SELECT SearchEngineID, ClientIP, count() AS c, sum(Refresh) FROM hits WHERE SearchPhrase != '' GROUP BY SearchEngineID, ClientIP ORDER BY SearchEngineID, ClientIP, c LIMIT 10;

-- q32
EXPLAIN verbose SELECT WatchID, ClientIP, count() AS c, sum(Refresh) FROM hits WHERE SearchPhrase != '' GROUP BY WatchID, ClientIP ORDER BY WatchID, ClientIP, c LIMIT 10;

-- q33
EXPLAIN verbose SELECT WatchID, ClientIP, count() AS c, sum(Refresh) FROM hits GROUP BY WatchID, ClientIP ORDER BY WatchID, ClientIP, c LIMIT 10;

-- q34
EXPLAIN verbose SELECT URL, count() AS c FROM hits GROUP BY URL ORDER BY url, c LIMIT 10;

-- q35
EXPLAIN verbose SELECT 1, URL, count() AS c FROM hits GROUP BY 1, URL ORDER BY url, c LIMIT 10;

-- q36
EXPLAIN verbose SELECT ClientIP AS x, ClientIP - 1 AS y, ClientIP - 2 AS z, ClientIP - 3 AS w, count() AS c FROM hits GROUP BY x, y, z, w ORDER BY x, y, z, w, c LIMIT 10;

-- q37
EXPLAIN verbose SELECT URL, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT DontCountHits = 1::smallint AND NOT Refresh = 1::smallint AND URL != '' GROUP BY URL ORDER BY PageViews LIMIT 10;

-- q38
EXPLAIN verbose SELECT Title, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT DontCountHits = 1::smallint AND NOT Refresh = 1::smallint AND Title != '' GROUP BY Title ORDER BY PageViews LIMIT 10;

-- q39
EXPLAIN verbose SELECT URL, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT Refresh = 1::smallint AND IsLink = 1::smallint AND NOT IsDownload = 1::smallint GROUP BY URL ORDER BY PageViews LIMIT 1000;

-- q40
EXPLAIN verbose SELECT TraficSourceID, SearchEngineID, AdvEngineID, URL AS Dst, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT Refresh = 1::smallint GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Dst ORDER BY PageViews LIMIT 1000;

-- q41
EXPLAIN verbose SELECT URLHash, EventDate, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT Refresh = 1::smallint AND (TraficSourceID = -1::smallint OR TraficSourceID = 6::smallint) GROUP BY URLHash, EventDate ORDER BY PageViews LIMIT 100;

-- q42
EXPLAIN verbose SELECT WindowClientWidth, WindowClientHeight, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT Refresh = 1::smallint AND NOT DontCountHits = 1::smallint GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews LIMIT 10000;

-- q43
EXPLAIN verbose SELECT EventTime, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-02'::DATE AND NOT Refresh = 1::smallint AND NOT DontCountHits = 1::smallint GROUP BY EventTime ORDER BY EventTime;

