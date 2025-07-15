

CREATE DATABASE airflow;
CREATE DATABASE financial_data;

CREATE USER airflow WITH PASSWORD 'airflow';
CREATE USER db_user WITH PASSWORD 'db_password';

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE financial_data TO db_user;

\c financial_data;

CREATE SCHEMA IF NOT EXISTS financial_data;
CREATE TABLE IF NOT EXISTS financial_data.ohlc_table(
    ticker VARCHAR(10),
    time_stamp TIMESTAMP,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    number_transactions BIGINT,
    volume BIGINT,
    volume_weighted_avg FLOAT,
    PRIMARY KEY(ticker, time_stamp)
);
