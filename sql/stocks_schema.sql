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
CREATE MATERIALIZED VIEW IF NOT EXISTS minmax AS
        SELECT ticker, min(close), max(close) FROM stock_data GROUP BY ticker;
-- https://wiki.postgresql.org/wiki/First/last_(aggregate)
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
