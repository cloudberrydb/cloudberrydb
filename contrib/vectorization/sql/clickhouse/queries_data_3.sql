
-- turn on vectorization
SET vector.enable_vectorization=on;
SET vector.max_batch_size = 128;

-- query 21-30

SELECT count() FROM hits WHERE URL LIKE '%metrika%';

SELECT SearchPhrase, min(URL), count() AS c FROM hits WHERE URL LIKE '%metrika%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY SearchPhrase, c LIMIT 10;

SELECT SearchPhrase, min(URL), min(Title), count() AS c, count(distinct UserID) FROM hits WHERE Title LIKE '%abc%' AND URL NOT LIKE '%.yandex.%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY SearchPhrase, c LIMIT 10;

SELECT URL FROM hits WHERE URL LIKE '%metrika%' ORDER BY EventTime LIMIT 10;

SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY EventTime LIMIT 10;

SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY SearchPhrase LIMIT 10;

SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY EventTime, SearchPhrase LIMIT 10;

SELECT CounterID, count() AS c FROM hits WHERE URL != '' GROUP BY CounterID HAVING count() > 100000 ORDER BY counterid, c LIMIT 25;

SELECT Referer AS key, count() AS c, min(Referer) FROM hits WHERE Referer != '' GROUP BY key ORDER BY key, c LIMIT 25;

SELECT sum(ResolutionWidth), sum(ResolutionWidth + 1), sum(ResolutionWidth + 2), sum(ResolutionWidth + 88), sum(ResolutionWidth + 89) FROM hits;

-- test gp optimizer
SELECT replace(Referer, 'www.', '') AS key, avg(length(Referer)) AS l, count(), min(Referer) FROM hits WHERE Referer != '' GROUP BY key HAVING count() > 40 ORDER BY l DESC LIMIT 25;
SET optimizer = off;
SELECT replace(Referer, 'www.', '') AS key, avg(length(Referer)) AS l, count(), min(Referer) FROM hits WHERE Referer != '' GROUP BY key HAVING count() > 40 ORDER BY l DESC LIMIT 25;
RESET optimizer;
