-- turn on vectorization

SET vector.enable_vectorization=on;

SET vector.max_batch_size = 128;


-- query 11 -20

SELECT MobilePhoneModel, count(distinct UserID) AS u FROM hits WHERE MobilePhoneModel != '' GROUP BY MobilePhoneModel ORDER BY MobilePhoneModel, u LIMIT 10;

SELECT MobilePhone, MobilePhoneModel, count(distinct UserID) AS u FROM hits WHERE MobilePhoneModel != '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY MobilePhone, MobilePhoneModel, u LIMIT 10;

SELECT SearchPhrase, count() AS c FROM hits WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY SearchPhrase, c LIMIT 10;

SELECT SearchPhrase, count(distinct UserID) AS u FROM hits WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY SearchPhrase, u LIMIT 10;

SELECT SearchEngineID, SearchPhrase, count() AS c FROM hits WHERE SearchPhrase != '' GROUP BY SearchEngineID, SearchPhrase ORDER BY SearchEngineID, SearchPhrase, c LIMIT 10;

SELECT UserID, count() FROM hits GROUP BY UserID ORDER BY userid, count() LIMIT 10;

SELECT UserID, SearchPhrase, count() FROM hits GROUP BY UserID, SearchPhrase ORDER BY UserID, SearchPhrase, count() LIMIT 10;

SELECT UserID, SearchPhrase, count() FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;

SELECT UserID, EventTime AS m, SearchPhrase, count() FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY UserID, m, SearchPhrase, count() LIMIT 10;

SELECT UserID FROM hits WHERE UserID = '1234567890123456789';

