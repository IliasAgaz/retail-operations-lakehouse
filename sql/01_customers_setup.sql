-- Reconstructed setup for a NEW demo database, not an export of Neon.
-- Matches notebook 07's required fields and validation rules.
-- Existing tables are not migrated by CREATE TABLE IF NOT EXISTS.
BEGIN;

CREATE SCHEMA IF NOT EXISTS retail;

CREATE TABLE IF NOT EXISTS retail.customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL CHECK (length(btrim(customer_name)) > 0),
    email TEXT NOT NULL CHECK (length(btrim(email)) > 0),
    country_code TEXT NOT NULL CHECK (country_code ~ '^[A-Z]{2}$'),
    customer_segment TEXT NOT NULL
        CHECK (customer_segment IN ('standard', 'premium', 'business')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (length(btrim(customer_id)) > 0),
    CHECK (updated_at >= created_at)
);

CREATE OR REPLACE FUNCTION retail.set_customer_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

-- Re-running this script replaces only this named trigger.
DROP TRIGGER IF EXISTS trg_customers_updated_at ON retail.customers;
CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON retail.customers
FOR EACH ROW
EXECUTE FUNCTION retail.set_customer_updated_at();

COMMIT;
