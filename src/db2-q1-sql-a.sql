-- load the dataset
COPY log_records FROM 'D:\VirtualBox Shared Folders\Linux Mint\db2_project_data.csv' WITH CSV HEADER DELIMITER ','
-- reload statistics
VACUUM FULL log_records
-- empty relation
TRUNCATE log_records
-- prewarm table for consistent time results
CREATE EXTENSION pg_prewarm;
SELECT pg_prewarm('log_records')


--1.a.i
Explain Analyze WITH KilometerSum (ClientID, sum_km) AS (
	SELECT id as ClientID, sum(kilometers) as sum_km
 	FROM log_records 
 	WHERE timestamp <='2016-12-05 06:00:00'
	GROUP BY ClientID
)
SELECT ClientID, sum_km
FROM KilometerSum
WHERE sum_km = (SELECT max(sum_km) FROM KilometerSum)

--1.a.ii
Explain Analyze SELECT avg(kilometers) FROM log_records
WHERE EXTRACT(month FROM timestamp) = EXTRACT(month FROM (SELECT max(timestamp) FROM log_records));

--1.a.iii
Explain Analyze SELECT id, EXTRACT(month FROM timestamp),sum(kilometers) FROM log_records
GROUP BY id, EXTRACT(month FROM timestamp);

--1.a.iv
Explain Analyze SELECT id,avg(kilometers) FROM log_records
GROUP BY id

--1.a.v
Explain Analyze SELECT region_id,avg(kilometers) FROM log_records
GROUP BY region_id
