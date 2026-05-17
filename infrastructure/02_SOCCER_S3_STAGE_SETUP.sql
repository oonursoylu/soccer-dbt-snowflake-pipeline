-- 1. Use the correct role and database context
USE ROLE TRANSFORM_ROLE;
USE DATABASE SOCCER_DB;
USE SCHEMA RAW;

-- 2. Create the external stage linking to your specific S3 bucket
CREATE OR REPLACE STAGE SOCCER_S3_STAGE
  URL = 's3://dbt-soccer-portfolio-oonur/'
  CREDENTIALS = (
    AWS_KEY_ID = '***' -- Masked for Git security! Never commit real AWS keys.
    AWS_SECRET_KEY = '***' -- Masked for Git security! Never commit real AWS keys.
  )
  COMMENT = 'External stage for S3 soccer data';

-- 3. Verify the connection by listing the files
LIST @SOCCER_S3_STAGE;