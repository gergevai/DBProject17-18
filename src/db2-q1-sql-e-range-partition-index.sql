--CREATE BTREE INDEX ON KILOMETERS
CREATE INDEX log_records_m12_kilometers_index ON log_records_m12 USING btree (kilometers);
CREATE INDEX log_records_m01_kilometers_index ON log_records_m01 USING btree (kilometers);
CREATE INDEX log_records_m02_kilometers_index ON log_records_m02 USING btree (kilometers);
CREATE INDEX log_records_m03_kilometers_index ON log_records_m03 USING btree (kilometers);
CREATE INDEX log_records_m04_kilometers_index ON log_records_m04 USING btree (kilometers);
CREATE INDEX log_records_m05_kilometers_index ON log_records_m05 USING btree (kilometers);
CREATE INDEX log_records_m06_kilometers_index ON log_records_m06 USING btree (kilometers);
CREATE INDEX log_records_m07_kilometers_index ON log_records_m07 USING btree (kilometers);

--CREATE BTREE INDEX ON TIMESTAMP
CREATE INDEX log_records_m12_timestamp_index ON log_records_m12 USING btree (timestamp DESC);
CREATE INDEX log_records_m01_timestamp_index ON log_records_m01 USING btree (timestamp DESC);
CREATE INDEX log_records_m02_timestamp_index ON log_records_m02 USING btree (timestamp DESC);
CREATE INDEX log_records_m03_timestamp_index ON log_records_m03 USING btree (timestamp DESC);
CREATE INDEX log_records_m04_timestamp_index ON log_records_m04 USING btree (timestamp DESC);
CREATE INDEX log_records_m05_timestamp_index ON log_records_m05 USING btree (timestamp DESC);
CREATE INDEX log_records_m06_timestamp_index ON log_records_m06 USING btree (timestamp DESC);
CREATE INDEX log_records_m07_timestamp_index ON log_records_m07 USING btree (timestamp DESC);

-- Make use of the Constraint Exclusion technique to Optimize Queries on Partitioned Tables 
-- defined in the fashion described in the file db2-q1-sql-range-partition.sql
SET constraint_exclusion = on;



--DROP BTREE INDEX ON KILOMETERS
DROP INDEX log_records_m12_kilometers_index;
DROP INDEX log_records_m01_kilometers_index;
DROP INDEX log_records_m02_kilometers_index;
DROP INDEX log_records_m03_kilometers_index;
DROP INDEX log_records_m04_kilometers_index;
DROP INDEX log_records_m05_kilometers_index;
DROP INDEX log_records_m06_kilometers_index;
DROP INDEX log_records_m07_kilometers_index;

--DROP BTREE INDEX ON TIMESTAMP
DROP INDEX log_records_m12_timestamp_index;
DROP INDEX log_records_m01_timestamp_index;
DROP INDEX log_records_m02_timestamp_index;
DROP INDEX log_records_m03_timestamp_index;
DROP INDEX log_records_m04_timestamp_index;
DROP INDEX log_records_m05_timestamp_index;
DROP INDEX log_records_m06_timestamp_index;
DROP INDEX log_records_m07_timestamp_index;
        