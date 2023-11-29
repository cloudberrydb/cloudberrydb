
-- turn on vectorization

SET vector.enable_vectorization=on;
SET vector.max_batch_size = 128;

-- query 41-43

SELECT URLHash, EventDate, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT Refresh = 1::smallint AND (TraficSourceID = -1::smallint OR TraficSourceID = 6::smallint) GROUP BY URLHash, EventDate ORDER BY PageViews LIMIT 100;

SELECT WindowClientWidth, WindowClientHeight, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND NOT Refresh = 1::smallint AND NOT DontCountHits = 1::smallint GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews LIMIT 10000;

SELECT EventTime, count() AS PageViews FROM hits WHERE CounterID = 34 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-02'::DATE AND NOT Refresh = 1::smallint AND NOT DontCountHits = 1::smallint GROUP BY EventTime ORDER BY EventTime;

