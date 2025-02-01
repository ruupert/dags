CREATE TABLE IF NOT EXISTS shelly_powers ( 
        time timestamptz not null,
        name TEXT,
        output boolean, 
        apower float,
        voltage float,
        freq float,
        current float,
        temperature float
);
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT create_hypertable('shelly_powers', by_range('time'), if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_name_time ON shelly_powers(name, time);
