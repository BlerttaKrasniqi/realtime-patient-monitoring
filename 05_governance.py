# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC USE CATALOG capstone_five;
# MAGIC
# MAGIC DESCRIBE TABLE EXTENDED bronze.dim_patients;

# COMMAND ----------

# MAGIC %md
# MAGIC ### PII access list

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS project.pii_access_list (
# MAGIC   user_email STRING NOT NULL,
# MAGIC   reason     STRING,
# MAGIC   added_ts   TIMESTAMP
# MAGIC ) COMMENT 'Users allowed to see unmasked patient PII';
# MAGIC
# MAGIC INSERT INTO project.pii_access_list VALUES
# MAGIC   ('gresa.nuredini@genpact.com', 'Data owner - ingestion', current_timestamp());

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mask functions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS project.pii_access_list (
# MAGIC   user_email STRING NOT NULL,
# MAGIC   reason     STRING,
# MAGIC   added_ts   TIMESTAMP
# MAGIC ) COMMENT 'Users allowed to see unmasked patient PII';
# MAGIC
# MAGIC MERGE INTO project.pii_access_list t
# MAGIC USING (SELECT 'gresa.nuredini@genpact.com' AS user_email, 'Data owner - ingestion' AS reason) s
# MAGIC ON lower(t.user_email) = lower(s.user_email)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (user_email, reason, added_ts) VALUES (s.user_email, s.reason, current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW silver.dim_patients_clinical
# MAGIC COMMENT 'PII-free patient attributes used by the streaming pipeline'
# MAGIC AS SELECT Id AS patient_id, GENDER, BIRTHDATE
# MAGIC    FROM bronze.dim_patients;
# MAGIC
# MAGIC SELECT * FROM silver.dim_patients_clinical LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply the mask

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze.dim_patients ALTER COLUMN FIRST   SET MASK project.mask_pii;
# MAGIC ALTER TABLE bronze.dim_patients ALTER COLUMN LAST    SET MASK project.mask_pii;
# MAGIC ALTER TABLE bronze.dim_patients ALTER COLUMN ADDRESS SET MASK project.mask_pii;
# MAGIC ALTER TABLE bronze.dim_patients ALTER COLUMN SSN     SET MASK project.mask_ssn;

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE bronze.dim_patients IS 'Synthea patient master (deduplicated). PII columns FIRST, LAST, SSN, ADDRESS are masked.';
# MAGIC COMMENT ON TABLE bronze.vitals       IS 'Raw IoT vitals ingested by Auto Loader, exactly-once via checkpoint.';
# MAGIC
# MAGIC ALTER TABLE bronze.dim_patients SET TAGS ('domain' = 'clinical', 'contains_pii' = 'true', 'layer' = 'bronze');
# MAGIC ALTER TABLE bronze.vitals       SET TAGS ('domain' = 'clinical', 'contains_phi' = 'true', 'layer' = 'bronze');
# MAGIC
# MAGIC ALTER TABLE bronze.dim_patients ALTER COLUMN FIRST   SET TAGS ('pii' = 'name');
# MAGIC ALTER TABLE bronze.dim_patients ALTER COLUMN LAST    SET TAGS ('pii' = 'name');
# MAGIC ALTER TABLE bronze.dim_patients ALTER COLUMN SSN     SET TAGS ('pii' = 'government_id');
# MAGIC ALTER TABLE bronze.dim_patients ALTER COLUMN ADDRESS SET TAGS ('pii' = 'address');
# MAGIC ALTER TABLE bronze.vitals       ALTER COLUMN patient_id SET TAGS ('phi' = 'patient_identifier');

# COMMAND ----------

# %sql
# GRANT USE CATALOG ON CATALOG capstone_five TO `gresa.nuredini@genpact.com`;
# GRANT USE CATALOG ON CATALOG capstone_five TO `<virender.email>@genpact.com`;

# GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA bronze TO `gresa.nuredini@genpact.com`;
# GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA silver TO `gresa.nuredini@genpact.com`;
# GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA gold   TO `gresa.nuredini@genpact.com`;
# GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA bronze TO `<virender.email>@genpact.com`;
# GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA silver TO `<virender.email>@genpact.com`;
# GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA gold   TO `<virender.email>@genpact.com`;

# GRANT USE SCHEMA ON SCHEMA project TO `gresa.nuredini@genpact.com`;
# GRANT USE SCHEMA ON SCHEMA project TO `<virender.email>@genpact.com`;
# GRANT READ VOLUME, WRITE VOLUME ON VOLUME project.landing     TO `gresa.nuredini@genpact.com`;
# GRANT READ VOLUME, WRITE VOLUME ON VOLUME project.checkpoints TO `gresa.nuredini@genpact.com`;
# GRANT READ VOLUME, WRITE VOLUME ON VOLUME project.landing     TO `<virender.email>@genpact.com`;
# GRANT READ VOLUME, WRITE VOLUME ON VOLUME project.checkpoints TO `<virender.email>@genpact.com`;

# GRANT SELECT ON TABLE project.pii_access_list TO `gresa.nuredini@genpact.com`;
# GRANT SELECT ON TABLE project.pii_access_list TO `<virender.email>@genpact.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Masking example

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS me,
# MAGIC        EXISTS (SELECT 1 FROM project.pii_access_list
# MAGIC                WHERE lower(user_email) = lower(current_user())) AS can_see_pii;
# MAGIC
# MAGIC SELECT Id, FIRST, LAST, SSN, ADDRESS, GENDER, BIRTHDATE
# MAGIC FROM bronze.dim_patients
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO project.pii_access_list t
# MAGIC USING (SELECT 'blerta.krasniqi@genpact.com' AS user_email, 'Governance demo' AS reason) s
# MAGIC ON lower(t.user_email) = lower(s.user_email)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (user_email, reason, added_ts) VALUES (s.user_email, s.reason, current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM project.pii_access_list WHERE user_email = 'blerta.krasniqi@genpact.com';

# COMMAND ----------

# DBTITLE 1,Lineage
# MAGIC %sql
# MAGIC SELECT source_table_full_name, target_table_full_name, entity_type, event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_catalog = 'capstone_five' OR target_table_catalog = 'capstone_five'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER STREAMING TABLE   capstone_five.silver.vitals     SET TAGS ('domain' = 'clinical', 'contains_phi' = 'true', 'layer' = 'silver');
# MAGIC ALTER STREAMING TABLE   capstone_five.gold.anomalies    SET TAGS ('domain' = 'clinical', 'contains_phi' = 'true', 'layer' = 'gold');
# MAGIC ALTER MATERIALIZED VIEW capstone_five.gold.vitals_1min  SET TAGS ('domain' = 'clinical', 'layer' = 'gold');