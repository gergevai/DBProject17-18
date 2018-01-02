--MASTER TABLE TRIGGER FOR INSERTING DATA ON PARTITIONS

-- drop function
DROP FUNCTION IF EXISTS timestamp_check();
-- create function
CREATE OR REPLACE FUNCTION timestamp_check()
RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.timestamp >= '2016-12-01 00:00:00' AND
         NEW.timestamp < '2017-01-01 00:00:00' ) THEN
        INSERT INTO log_records_m12 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-01-01 00:00:00' AND
            NEW.timestamp < '2017-02-01 00:00:00' ) THEN
        INSERT INTO log_records_m01 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-02-01 00:00:00' AND
            NEW.timestamp < '2017-03-01 00:00:00' ) THEN
        INSERT INTO log_records_m02 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-03-01 00:00:00' AND
            NEW.timestamp < '2017-04-01 00:00:00' ) THEN
        INSERT INTO log_records_m03 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-04-01 00:00:00' AND
            NEW.timestamp < '2017-05-01 00:00:00' ) THEN
        INSERT INTO log_records_m04 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-05-01 00:00:00' AND
            NEW.timestamp < '2017-06-01 00:00:00' ) THEN
        INSERT INTO log_records_m05 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-06-01 00:00:00' AND
            NEW.timestamp < '2017-07-01 00:00:00' ) THEN
        INSERT INTO log_records_m06 VALUES (NEW.*);
    ELSIF ( NEW.timestamp >= '2017-07-01 00:00:00' AND
            NEW.timestamp < '2017-08-01 00:00:00' ) THEN
        INSERT INTO log_records_m07 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Date out of range.  Fix the measurement_insert_trigger() function!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

-- drop trigger (if exists)
DROP TRIGGER IF EXISTS timestamp_trigger ON log_records;
-- create trigger
CREATE TRIGGER timestamp_trigger
BEFORE INSERT ON log_records
FOR EACH ROW EXECUTE PROCEDURE timestamp_check();


