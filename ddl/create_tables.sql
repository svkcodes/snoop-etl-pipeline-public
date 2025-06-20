-- ddl/create_tables.sql

CREATE TABLE IF NOT EXISTS transactions (
    customerId VARCHAR NOT NULL,
    transactionId VARCHAR NOT NULL,
    transactionDate DATE,
    sourceDate TIMESTAMP,
    merchantId VARCHAR,
    categoryId VARCHAR,
    amount NUMERIC,
    description TEXT,
    currency VARCHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customerId, transactionId)
);

CREATE TABLE IF NOT EXISTS customers (
    customerId VARCHAR PRIMARY KEY,
    latestTransactionDate DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS error_log (
    id SERIAL PRIMARY KEY,
    customerId VARCHAR,
    transactionId VARCHAR,
    error_reason TEXT,
    raw_data JSONB,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
