-- PostgreSQL initialization script for ETL pipeline testing
-- Creates sample tables for source and sink operations

-- Create users table (source table)
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user_events table (sink table)
CREATE TABLE IF NOT EXISTS user_events (
    event_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_timestamp BIGINT NOT NULL,
    properties JSONB,
    amount DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user_summary table (aggregation sink)
CREATE TABLE IF NOT EXISTS user_summary (
    user_id VARCHAR(50) PRIMARY KEY,
    total_events BIGINT NOT NULL,
    total_amount DOUBLE PRECISION,
    first_event_timestamp BIGINT,
    last_event_timestamp BIGINT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create transactions table (source table for join operations)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    transaction_timestamp BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data into users table
INSERT INTO users (user_id, username, email) VALUES
    ('user-001', 'alice', 'alice@example.com'),
    ('user-002', 'bob', 'bob@example.com'),
    ('user-003', 'charlie', 'charlie@example.com'),
    ('user-004', 'diana', 'diana@example.com'),
    ('user-005', 'eve', 'eve@example.com')
ON CONFLICT (user_id) DO NOTHING;

-- Insert sample transactions
INSERT INTO transactions (transaction_id, user_id, amount, transaction_type, transaction_timestamp) VALUES
    ('txn-001', 'user-001', 99.99, 'PURCHASE', 1704067200000),
    ('txn-002', 'user-001', 49.99, 'PURCHASE', 1704153600000),
    ('txn-003', 'user-002', 199.99, 'PURCHASE', 1704240000000),
    ('txn-004', 'user-003', 29.99, 'REFUND', 1704326400000),
    ('txn-005', 'user-002', 149.99, 'PURCHASE', 1704412800000)
ON CONFLICT (transaction_id) DO NOTHING;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_user_events_timestamp ON user_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(transaction_timestamp);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO etl_user;
