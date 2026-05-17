-- 1. Ensure we are using the correct role, database, and schema
USE ROLE TRANSFORM_ROLE;
USE DATABASE SOCCER_DB;
USE SCHEMA RAW;

-- 2. Create a file format to tell Snowflake how to parse our CSV files
CREATE OR REPLACE FILE FORMAT SOCCER_CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMMENT = 'Format for parsing comma-separated soccer data';

-- 3. Peek into the S3 file directly without loading the data!
SELECT $1, $2, $3, $4, $5 
FROM @SOCCER_S3_STAGE/teams.csv 
(FILE_FORMAT => SOCCER_CSV_FORMAT) 
LIMIT 5;