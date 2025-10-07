-- MySQL initialization script for ETL pipeline testing
-- Creates sample tables for source and sink operations

USE etl_database;

-- Create users table (source table)
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create user_events table (sink table)
CREATE TABLE IF NOT EXISTS user_events (
    event_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_timestamp BIGINT NOT NULL,
    properties JSON,
    amount DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_timestamp (event_timestamp)
);

-- Create user_summary table (aggregation sink)
CREATE TABLE IF NOT EXISTS user_summary (
    user_id VARCHAR(50) PRIMARY KEY,
    total_events BIGINT NOT NULL,
    total_amount DOUBLE,
    first_event_timestamp BIGINT,
    last_event_timestamp BIGINT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create product_catalog table (dimensional data for joins)
CREATE TABLE IF NOT EXISTS product_catalog (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DOUBLE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data into users table
INSERT IGNORE INTO users (user_id, username, email) VALUES
    ('user-001', 'alice', 'alice@example.com'),
    ('user-002', 'bob', 'bob@example.com'),
    ('user-003', 'charlie', 'charlie@example.com'),
    ('user-004', 'diana', 'diana@example.com'),
    ('user-005', 'eve', 'eve@example.com');

-- Insert sample product catalog
INSERT IGNORE INTO product_catalog (product_id, product_name, category, price) VALUES
    ('prod-001', 'Laptop', 'Electronics', 999.99),
    ('prod-002', 'Mouse', 'Electronics', 29.99),
    ('prod-003', 'Keyboard', 'Electronics', 79.99),
    ('prod-004', 'Monitor', 'Electronics', 299.99),
    ('prod-005', 'Headphones', 'Electronics', 149.99);
