COPY maint FROM 'D:\db2_project_data\Test.csv' WITH CSV HEADER DELIMITER ','
VACUUM FULL maint
TRUNCATE maint
--1.B
ALTER SYSTEM SET shared_buffers TO '2GB'
--1.C
ALTER SYSTEM SET max_worker_processes TO '16'
ALTER SYSTEM SET max_parallel_workers_per_gather TO '16'
ALTER SYSTEM SET dynamic_shared_memory_type TO 'windows'
SELECT pg_reload_conf();

--1.A.i
SELECT id,max(kilometers) FROM maint 
WHERE timestamp <='2016-12-05 06:00:00' AND kilometers = (
    SELECT max(kilometers) 
 	FROM maint 
 	WHERE timestamp <='2016-12-05 06:00:00')
GROUP BY id;

--1.A.ii
SELECT avg(kilometers) FROM maint
WHERE EXTRACT(month FROM timestamp) = EXTRACT(month FROM (SELECT max(timestamp) FROM maint));

SELECT avg(kilometers) FROM maint
WHERE EXTRACT(month FROM timestamp) = EXTRACT(month FROM (SELECT timestamp FROM maint ORDER BY timestamp DESC LIMIT 1));

SELECT avg(kilometers) FROM maint
WHERE EXTRACT(month FROM timestamp) = 7

SELECT EXTRACT(month FROM (SELECT timestamp FROM maint ORDER BY timestamp DESC LIMIT 1));

--1.A.iii
SELECT id, EXTRACT(month FROM timestamp),sum(kilometers) FROM maint
GROUP BY id, EXTRACT(month FROM timestamp);

--1.A.iv
SELECT id,avg(kilometers) FROM maint
GROUP BY id

--1.A.v
SELECT region_id,avg(kilometers) FROM maint
GROUP BY region_id

CREATE INDEX ind_id ON maint USING HASH (id);
CREATE INDEX ind_time ON maint (timestamp);
CREATE INDEX ind_kilos ON maint (kilometers);
CREATE INDEX ind_regionid ON maint (region_id);

DROP INDEX ind_id;
DROP INDEX ind_time;
DROP INDEX ind_kilos;
DROP INDEX ind_regionid;



