

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
\q


-- docker exec -it $(docker-compose ps -q postgres) psql -U postgres -c "\c financial_data"
-- 1. Start PostgreSQL first
-- docker-compose up -d postgres

-- 2. Wait for PostgreSQL to be ready
-- docker-compose logs postgres
-- 3. Connect and verify database setup
-- docker exec -it $(docker-compose ps -q postgres) psql -U postgres -d financial_data -c "\dt financial_data.*"