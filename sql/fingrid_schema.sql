CREATE TABLE IF NOT EXISTS fingrid_links (id BIGSERIAL PRIMARY KEY, name text);
CREATE TABLE IF NOT EXISTS fingrid_links (id INTEGER, name text);
CREATE TABLE IF NOT EXISTS fingrid_data (time TIMESTAMP not null, dataset_id INTEGER, value FLOAT);
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT create_hypertable('fingrid_data', by_range('time'), if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fingrid_links_id ON fingrid_links (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fingrid_data_time_dataset_id ON fingrid_data (time, dataset_id);
