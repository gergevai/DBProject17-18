CREATE INDEX id_index ON log_records USING HASH (id);
CREATE INDEX timestamp_index ON log_records (timestamp);
CREATE INDEX kilometers_index ON log_records (kilometers);
CREATE INDEX region_id_index ON log_records USING HASH (region_id);

DROP INDEX id_index;
DROP INDEX kilometers_index;
DROP INDEX region_id_index;
DROP INDEX timestamp_index;