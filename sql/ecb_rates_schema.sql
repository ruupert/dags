CREATE TABLE IF NOT EXISTS ecb_rate_eur ( 
        time timestamp not null, 
        currency text, 
        rate FLOAT 
);
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT create_hypertable('ecb_rate_eur', by_range('time'), if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ecb_eur_time_curr on ecb_rate_eur(currency, time);
