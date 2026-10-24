# PostgreSQL customer source

These scripts reconstruct the demo setup from the notebook contract. They are
not a database export, and the synthetic customer attributes may differ from
the original Neon database.

For a new database, run `01_customers_setup.sql`, then
`02_customers_demo_seed.sql` in the Neon SQL editor. The seed creates C001–C020
without replacing existing IDs. Do not run the setup against an existing
database without inspecting its schema and triggers: it does not migrate an
existing table, and another update trigger may already exist.

Notebook 07 expects customer_id, customer_name, email, country_code,
customer_segment, is_active, created_at, and updated_at. The update trigger
refreshes updated_at for query-based incremental ingestion. Configure the
customer ingestion pipeline with customer_id as its primary key and updated_at
as its cursor column.

Query-based capture observes snapshots. It is not a complete change log and
does not preserve every intermediate update or detect hard deletes through
this timestamp alone. Timestamp polling also requires the connector's cursor
semantics to handle transaction timing correctly.

Validation status: checked against notebook 07's fields and rules; not executed
against PostgreSQL or Neon in this session. No credentials are included.
