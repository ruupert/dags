CREATE TABLE IF NOT EXISTS tickers (id serial, ticker text not null, sheet text not null, PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS prices (date timestamp, price float not null, ticker_id int not null, CONSTRAINT fk_ticker FOREIGN KEY(ticker_id) REFERENCES tickers(id));
CREATE UNIQUE INDEX IF NOT EXISTS idx_stocks_tickers ON tickers (ticker, sheet);
CREATE UNIQUE INDEX IF NOT EXISTS idx_stocks_prices ON prices (date, price, ticker_id);
-- populate tickers
INSERT INTO tickers VALUES ('ANORA', '12OwC5VqDU261UUIz3g2PlcKZrjyIalDI7YbCVMqjuFY');
INSERT INTO tickers VALUES ('CCL', '1_W7Nno_hyjIeCu4E6VodzNe0sg2L_lBgXe9A2EhZggs');
INSERT INTO tickers VALUES ('CA', '1QkUiWwP_0WPShfnH-4h2ni5ggf6qsq5nJZYfNhT89uw');
INSERT INTO tickers VALUES ('LHA', '1R5IfwmDB0kPoGI3FG2jqQu4CaWllTxr35lIaxXIiNlQ');
INSERT INTO tickers VALUES ('FORTUM', '1cyWMMh9qsd81aH3-wg8Fy8__BjV_j2nfjkHrrnnRjTA');
INSERT INTO tickers VALUES ('GSK', '1vSs6qF3QLwY-rWfLwbzx4hcUBnxlDCo1E7rFPfY2hco');
INSERT INTO tickers VALUES ('HSBA', '1Y7QqJBNMT-j2lMnqU2L3aAixhnMgwPyxgM0MisWzLNo');
INSERT INTO tickers VALUES ('KESKOB', '1uShpYDges1RfwuhBzky-YoDMCrFhDZXni38qrrwoPxo');
INSERT INTO tickers VALUES ('KNEBV', '1LYa3yvW3mJodiLJziQ6orS9pXXn9kLiS1anrUyzAgyw');
INSERT INTO tickers VALUES ('NDA-FI', '1gqDIGdPO_WsnRhD2AEpih0Wkr6b0bFnLhgZ1J09bvbA');
INSERT INTO tickers VALUES ('NOVN', '1g4sNRG-wS3owGDA88wco3NnHhk9AjhCJpKSyr_zzd1k');
INSERT INTO tickers VALUES ('TELIA', '14QdjzggtDSeu1H-UyDQmr_c4VSlSMPOUnj5rjfDTUgE');
INSERT INTO tickers VALUES ('ULVR', '1-7yAdt6dLDymC78QKk0y983qECGBRLKwYTAQkOxKv8A');
INSERT INTO tickers VALUES ('VIK1V', '1B6N5U-l6a86KU1DXBpk6w-ciqT-9sM-N2wo-1VYMspI');
