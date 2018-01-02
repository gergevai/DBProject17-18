-- CREATE RANGE PARTITIONS
-- each partition is one month in range
CREATE TABLE log_records_m12 (
	CHECK (((timestamp >= '2016-12-01 00:00:00') AND (timestamp < '2016-12-31 23:59:59')))
) INHERITS (log_records);

CREATE TABLE log_records_m01 (
	CHECK (((timestamp>='2017-01-01 00:00:00') AND (timestamp<='2017-01-31 23:59:59')))
) INHERITS (log_records);

CREATE TABLE log_records_m02 (
	CHECK (((timestamp>='2017-02-01 00:00:00') AND (timestamp<='2017-02-28 23:59:59')))
) INHERITS (log_records);

CREATE TABLE log_records_m03 (
	CHECK (((timestamp>='2017-03-01 00:00:00') AND (timestamp<='2017-03-31 23:59:59')))
) INHERITS (log_records);

CREATE TABLE log_records_m04 (
	CHECK (((timestamp>='2017-04-01 00:00:00') AND (timestamp<='2017-04-30 23:59:59')))
) INHERITS (log_records);

CREATE TABLE log_records_m05 (
	CHECK (((timestamp>='2017-05-01 00:00:00') AND (timestamp<='2017-05-31 23:59:59')))
) INHERITS (log_records);

CREATE TABLE log_records_m06 (
	CHECK (((timestamp>='2017-06-01 00:00:00') AND (timestamp<='2017-06-30 23:59:59')))
) INHERITS (log_records);

CREATE TABLE log_records_m07 (
	CHECK (((timestamp>='2017-07-01 00:00:00') AND (timestamp<='2017-07-31 23:59:59')))
) INHERITS (log_records);


-- DROP PARTITIONS
DROP TABLE log_records_m12;
DROP TABLE log_records_m01;
DROP TABLE log_records_m02;
DROP TABLE log_records_m03;
DROP TABLE log_records_m04;
DROP TABLE log_records_m05;
DROP TABLE log_records_m06;
DROP TABLE log_records_m07;


-- TRUNCATE PARTITIONS
TRUNCATE TABLE log_records_m12;
TRUNCATE TABLE log_records_m01;
TRUNCATE TABLE log_records_m02;
TRUNCATE TABLE log_records_m03;
TRUNCATE TABLE log_records_m04;
TRUNCATE TABLE log_records_m05;
TRUNCATE TABLE log_records_m06;
TRUNCATE TABLE log_records_m07;

-- VACUUM FULL PARTITIONS
VACUUM FULL log_records_m12;
VACUUM FULL log_records_m01;
VACUUM FULL log_records_m02;
VACUUM FULL log_records_m03;
VACUUM FULL log_records_m04;
VACUUM FULL log_records_m05;
VACUUM FULL log_records_m06;
VACUUM FULL log_records_m07;