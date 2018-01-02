--CREATE RANGE PARTITIONS
--each partition is one month in range
CREATE TABLE p1 (
CONSTRAINT timestamp_date CHECK (((timestamp >= '2016-12-01 00:00:00') AND (timestamp < '2016-12-31 23:59:59')))
) INHERITS (log_records);
CREATE TABLE p2 (
CONSTRAINT timestamp_date CHECK (((timestamp>='2017-01-01 00:00:00') AND (timestamp<='2017-01-31 23:59:59')))
) INHERITS (log_records);
CREATE TABLE p3 (
CONSTRAINT timestamp_date CHECK (((timestamp>='2017-02-01 00:00:00') AND (timestamp<='2017-02-28 23:59:59')))
) INHERITS (log_records);
CREATE TABLE p4 (
CONSTRAINT timestamp_date CHECK (((timestamp>='2017-03-01 00:00:00') AND (timestamp<='2017-03-31 23:59:59')))
) INHERITS (log_records);
CREATE TABLE p5 (
CONSTRAINT timestamp_date CHECK (((timestamp>='2017-04-01 00:00:00') AND (timestamp<='2017-04-30 23:59:59')))
) INHERITS (log_records);
CREATE TABLE p6 (
CONSTRAINT timestamp_date CHECK (((timestamp>='2017-05-01 00:00:00') AND (timestamp<='2017-05-31 23:59:59')))
) INHERITS (log_records);
CREATE TABLE p7 (
CONSTRAINT timestamp_date CHECK (((timestamp>='2017-06-01 00:00:00') AND (timestamp<='2017-06-30 23:59:59')))
) INHERITS (log_records);
CREATE TABLE p8 (
CONSTRAINT timestamp_date CHECK (((timestamp>='2017-07-01 00:00:00') AND (timestamp<='2017-07-31 23:59:59')))
) INHERITS (log_records);

--DROP PARTITIONS
DROP TABLE p1;
DROP TABLE p2;
DROP TABLE p3;
DROP TABLE p4;
DROP TABLE p5;
DROP TABLE p6;
DROP TABLE p7;
DROP TABLE p8;

--TRUNCATE PARTITIONS
TRUNCATE TABLE p1;
TRUNCATE TABLE p2;
TRUNCATE TABLE p3;
TRUNCATE TABLE p4;
TRUNCATE TABLE p5;
TRUNCATE TABLE p6;
TRUNCATE TABLE p7;
TRUNCATE TABLE p8;

--FILL PARTITIONS
CREATE OR REPLACE FUNCTION timestamp_check()
RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.timestamp >= '2016-12-01 00:00:00' AND
         NEW.timestamp < '2017-01-01 00:00:00' ) THEN
        INSERT INTO p1 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-01-01 00:00:00' AND
            NEW.timestamp < '2017-02-01 00:00:00' ) THEN
        INSERT INTO p2 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-02-01 00:00:00' AND
            NEW.timestamp < '2017-03-01 00:00:00' ) THEN
        INSERT INTO p3 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-03-01 00:00:00' AND
            NEW.timestamp < '2017-04-01 00:00:00' ) THEN
        INSERT INTO p4 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-04-01 00:00:00' AND
            NEW.timestamp < '2017-05-01 00:00:00' ) THEN
        INSERT INTO p5 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-05-01 00:00:00' AND
            NEW.timestamp < '2017-06-01 00:00:00' ) THEN
        INSERT INTO p6 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-06-01 00:00:00' AND
            NEW.timestamp < '2017-07-01 00:00:00' ) THEN
        INSERT INTO p7 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-07-01 00:00:00' AND
            NEW.timestamp < '2017-08-01 00:00:00' ) THEN
        INSERT INTO p8 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Date out of range.  Fix the measurement_insert_trigger() function!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

DROP FUNCTION timestamp_check()

CREATE TRIGGER timestamp_trigger
    BEFORE INSERT ON log_records
    FOR EACH ROW EXECUTE PROCEDURE timestamp_check();

DROP TRIGGER timestamp_trigger

--CREATE BTREE INDEX ON KILOMETERS
CREATE INDEX p1_kilos ON p1 USING btree (kilometers);
CREATE INDEX p2_kilos ON p2 USING btree (kilometers);
CREATE INDEX p3_kilos ON p3 USING btree (kilometers);
CREATE INDEX p4_kilos ON p4 USING btree (kilometers);
CREATE INDEX p5_kilos ON p5 USING btree (kilometers);
CREATE INDEX p6_kilos ON p6 USING btree (kilometers);
CREATE INDEX p7_kilos ON p7 USING btree (kilometers);
CREATE INDEX p8_kilos ON p8 USING btree (kilometers);

--DROP BTREE INDEX ON KILOMETERS
DROP INDEX p1_kilos;
DROP INDEX p2_kilos;
DROP INDEX p3_kilos;
DROP INDEX p4_kilos;
DROP INDEX p5_kilos;
DROP INDEX p6_kilos;
DROP INDEX p7_kilos;
DROP INDEX p8_kilos;

--CREATE BTREE INDEX ON TIMESTAMP
CREATE INDEX p1_time ON p1 USING btree (timestamp DESC);
CREATE INDEX p2_time ON p2 USING btree (timestamp DESC);
CREATE INDEX p3_time ON p3 USING btree (timestamp DESC);
CREATE INDEX p4_time ON p4 USING btree (timestamp DESC);
CREATE INDEX p5_time ON p5 USING btree (timestamp DESC);
CREATE INDEX p6_time ON p6 USING btree (timestamp DESC);
CREATE INDEX p7_time ON p7 USING btree (timestamp DESC);
CREATE INDEX p8_time ON p8 USING btree (timestamp DESC);

--DROP BTREE INDEX ON TIMESTAMP
DROP INDEX p1_time;
DROP INDEX p2_time;
DROP INDEX p3_time;
DROP INDEX p4_time;
DROP INDEX p5_time;
DROP INDEX p6_time;
DROP INDEX p7_time;
DROP INDEX p8_time;
        























