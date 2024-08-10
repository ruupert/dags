-- testing first inserting tickers as text, later as ints and link table strings.
CREATE TABLE IF NOT EXISTS stock_data ( 
        time timestamp not null, 
        open FLOAT, 
        high FLOAT, 
        low FLOAT, 
        close FLOAT, 
        adjclose FLOAT,
        repaired boolean,
        volume FLOAT, 
        ticker TEXT 
);
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT create_hypertable('stock_data', by_range('time'), if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ticker_time on stock_data(ticker, time);
