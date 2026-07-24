BEGIN TRANSACTION;

-- 1. Safely empty target (retaining partition, cluster, and option properties)
TRUNCATE TABLE `{project_id}.{dw_dataset}.editions`;

-- 2. Extract data from raw, transform, and populate target
INSERT INTO `{project_id}.{dw_dataset}.editions` (
  id,
  created_at,
  updated_at,
  document_id,
  state,
  type,
  major_change_published_at,
  first_published_at,
  force_published,
  public_timestamp,
  scheduled_publication,
  access_limited,
  opening_at,
  closing_at,
  political,
  primary_locale,
  auth_bypass_id,
  government_id,
  slug
)
SELECT
  id,
  created_at,
  updated_at,
  document_id,
  state,
  type,
  major_change_published_at,
  -- 1. Convert STRING format to proper TIMESTAMPS safely 
  SAFE_CAST(first_published_at AS TIMESTAMP) AS first_published_at,
  -- 2. Convert INT64 to BOOLEAN (0 -> FALSE, non-zero -> TRUE)
  CAST(force_published AS BOOL) AS force_published,
  -- 3. Convert STRING to TIMESTAMP safely
  SAFE_CAST(public_timestamp AS TIMESTAMP) AS public_timestamp,
  scheduled_publication,
  -- 4. Convert INT64 to BOOLEAN 
  CAST(access_limiting AS BOOL) AS access_limited,
  opening_at,
  closing_at,
  -- 5. Convert FLOAT64 to BOOLEAN.
  -- (Since BigQuery cannot cast FLOAT64 directly to BOOL, a CASE statement is the safest method)
  CASE 
    WHEN political IS NULL THEN NULL
    WHEN political = 0.0 THEN FALSE
    ELSE TRUE 
  END AS political,
  primary_locale,
  auth_bypass_id,
  government_id,
  slug
FROM `{project_id}.{raw_dataset}.editions`;

COMMIT TRANSACTION;