--- create price table
CREATE TABLE IF NOT EXISTS price (date timestamp NOT NULL, val REAL NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_price_date ON price (date);
--- create consumption table
CREATE TABLE IF NOT EXISTS consumption (date timestamp NOT NULL, val REAL NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_consumption_date ON consumption (date);
