-- Reconstructed synthetic fixtures; these are not the original Neon rows.
-- Run after 01_customers_setup.sql in a new demo database.
-- Existing customer IDs are preserved when this script is rerun.
INSERT INTO retail.customers (
    customer_id, customer_name, email, country_code,
    customer_segment, is_active
)
SELECT
    'C' || lpad(i::text, 3, '0'),
    'Demo Customer ' || lpad(i::text, 3, '0'),
    'customer' || lpad(i::text, 3, '0') || '@example.com',
    'GB',
    CASE WHEN i % 3 = 0 THEN 'business'
         WHEN i % 3 = 1 THEN 'standard'
         ELSE 'premium' END,
    TRUE
FROM generate_series(1, 20) AS sample(i)
ON CONFLICT (customer_id) DO NOTHING;

SELECT customer_id, customer_name, customer_segment, updated_at
FROM retail.customers
ORDER BY customer_id;
