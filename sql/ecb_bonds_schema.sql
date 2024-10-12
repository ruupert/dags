CREATE TABLE IF NOT EXISTS ecb_bonds ( 
        time timestamp not null, 
        bond text, 
        yield FLOAT 
);
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT create_hypertable('ecb_bonds', by_range('time'), if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ecb_time_bond on ecb_bonds(bond, time);
