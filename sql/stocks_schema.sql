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
CREATE UNIQUE INDEX IF NOT EXISTS idx_ticker_time ON stock_data(ticker, time);
CREATE MATERIALIZED VIEW IF NOT EXISTS minmax AS
        SELECT ticker, min(close), max(close) FROM stock_data GROUP BY ticker;
CREATE OR REPLACE FUNCTION public.first_agg (anyelement, anyelement)
  RETURNS anyelement
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
'SELECT $1';
CREATE OR REPLACE AGGREGATE public.first (anyelement) (
  SFUNC    = public.first_agg
, STYPE    = anyelement
, PARALLEL = safe
);
CREATE OR REPLACE FUNCTION public.last_agg (anyelement, anyelement)
  RETURNS anyelement
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
'SELECT $2';
CREATE OR REPLACE AGGREGATE public.last (anyelement) (
  SFUNC    = public.last_agg
, STYPE    = anyelement
, PARALLEL = safe
);
CREATE MATERIALIZED VIEW IF NOT EXISTS last_close AS
        select ticker, last(close) from stock_data group by ticker;
