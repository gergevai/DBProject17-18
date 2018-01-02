COPY log_records FROM 'D:\VirtualBox Shared Folders\Linux Mint\db2_project_data.csv' WITH CSV HEADER DELIMITER ','
VACUUM FULL log_records
TRUNCATE log_records

CREATE EXTENSION pg_prewarm;
SELECT pg_prewarm('log_records')


SELECT region_id, count(id)/(SELECT count(*) FROM log_records)::float*100
FROM log_records
GROUP BY region_id

SELECT region_id, count(id)
FROM log_records
GROUP BY region_id

select count (*) from log_records


select EXTRACT(month FROM timestamp) as foo, sum(kilometers)/(SELECT sum(kilometers) FROM log_records)::float*100
FROM log_records
group by foo


select region_id, EXTRACT(month FROM timestamp) as foo, avg(kilometers)
FROM log_records
group by region_id, foo

-- Database Size
SELECT pg_size_pretty(pg_database_size('postgres'));
-- Table Size
SELECT pg_size_pretty(pg_relation_size('log_records'));

-- FOR PARTITIONING OPTIMIZATION SET constraint_exclusion = on;

--1.A.i
SELECT id,max(kilometers) FROM log_records 
WHERE timestamp <='2016-12-05 06:00:00' AND kilometers = (
    SELECT max(kilometers) 
 	FROM log_records 
 	WHERE timestamp <='2016-12-05 06:00:00')
GROUP BY id;

SET constraint_exclusion = on;
Explain Analyze WITH KilometerSum (ClientID, sum_km) AS (
	SELECT id as ClientID, sum(kilometers) as sum_km
 	FROM log_records 
 	WHERE timestamp <='2016-12-05 06:00:00'
	GROUP BY ClientID
)
SELECT ClientID, sum_km
FROM KilometerSum
WHERE sum_km = (SELECT max(sum_km) FROM KilometerSum)

--1.A.ii
Explain Analyze SELECT avg(kilometers) FROM log_records
WHERE EXTRACT(month FROM timestamp) = EXTRACT(month FROM (SELECT max(timestamp) FROM log_records));
-----------------------------------
--SELECT avg(kilometers) FROM maint
--WHERE EXTRACT(month FROM timestamp) = EXTRACT(month FROM (SELECT timestamp FROM maint ORDER BY timestamp DESC LIMIT 1));
-----------------------------------
--SELECT avg(kilometers) FROM maint
--WHERE EXTRACT(month FROM timestamp) = 7
-----------------------------------
--SELECT EXTRACT(month FROM (SELECT timestamp FROM maint ORDER BY timestamp DESC LIMIT 1));

--1.A.iii
Explain Analyze SELECT id, EXTRACT(month FROM timestamp),sum(kilometers) FROM log_records
GROUP BY id, EXTRACT(month FROM timestamp);

--1.A.iv
Explain Analyze SELECT id,avg(kilometers) FROM log_records
GROUP BY id

--1.A.v
Explain Analyze SELECT region_id,avg(kilometers) FROM log_records
GROUP BY region_id


--1.B
	-- DEFAULT OPTION
ALTER SYSTEM SET shared_buffers TO '128MB' -- CHOSEN ONE
	-- SEVERAL OPTIONS
ALTER SYSTEM SET shared_buffers TO '2GB'    --WIP
ALTER SYSTEM SET shared_buffers TO '2560MB' --WIP
ALTER SYSTEM SET shared_buffers TO '3277MB' --WIP
	-- OUR OPTION
ALTER SYSTEM SET shared_buffers TO '2GB'
	--Reload PostgreSQL Configuration
SELECT pg_reload_conf();


--1.C
	--DEFAULT & OUR OPTION
ALTER SYSTEM SET max_worker_processes TO '8'
----
    --DEFAULT OPTION
ALTER SYSTEM SET max_parallel_workers_per_gather TO '2'
    --OUR OPTION
ALTER SYSTEM SET max_parallel_workers_per_gather TO '8'
----
	--DEFAULT & OUR OPTION
ALTER SYSTEM SET dynamic_shared_memory_type TO 'windows'
----
	--Reload PostgreSQL Configuration
SELECT pg_reload_conf();






CREATE INDEX id_index ON log_records USING HASH (id);
CREATE INDEX timestamp_index ON log_records (timestamp);
CREATE INDEX kilometers_index ON log_records (kilometers);
--CREATE INDEX region_id_index ON log_records (region_id);
CREATE INDEX region_id_index ON log_records USING HASH (region_id);

DROP INDEX id_index;
DROP INDEX kilometers_index;
DROP INDEX region_id_index;
DROP INDEX timestamp_index;



