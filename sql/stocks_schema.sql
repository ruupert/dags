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
-- alter table did not seem to have set if not exist
-- ALTER TABLE stock_data SET ( timescaledb.compress, timescaledb.compress_orderby = 'time' );
-- SELECT add_compression_policy('stock_data', INTERVAL '7 days', if_not_exists => true);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ticker_time on stock_data(ticker, time);
