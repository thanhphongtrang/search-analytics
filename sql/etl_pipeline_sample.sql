-- ============================================================================
-- ETL PIPELINE SAMPLE: GA4 TO DATABRICKS TO CURATED TABLES
-- ============================================================================
-- Business Context:
-- Daily batch job processing 10k+ GA4 search events across 100+ markets
-- Integrates with Snowflake order data to enable complete search-to-order analysis
--
-- Architecture: GA4 → Databricks ETL → Curated Tables → Power BI
-- Schedule: Daily at 2 AM UTC (incremental load)
-- ============================================================================

-- ============================================================================
-- STEP 1: EXTRACT - Load raw GA4 events from previous day
-- ============================================================================
-- Assumes GA4 data is already loaded into Databricks landing zone via connector

CREATE OR REPLACE TEMPORARY VIEW ga4_events_raw AS
SELECT 
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    -- Parse event parameters into columns
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'event_category') as event_category,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'event_action') as event_action,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'event_label') as event_label,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'search_term') as search_term,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'search_result_count') as search_result_count,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_name') as page_name,
    geo.country as country_code,
    device.category as device_category,
    traffic_source.source as traffic_source
FROM `project.dataset.events_*`
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE - 1)
    AND event_category = 'global search';

-- ============================================================================
-- STEP 2: TRANSFORM - Clean and enrich GA4 events
-- ============================================================================

-- 2A: Deduplicate events (GA4 can have duplicates)
CREATE OR REPLACE TEMPORARY VIEW ga4_events_deduped AS
SELECT DISTINCT
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    event_category,
    event_action,
    event_label,
    search_term,
    search_result_count,
    page_name,
    country_code,
    device_category,
    traffic_source
FROM ga4_events_raw
WHERE event_timestamp IS NOT NULL
    AND user_pseudo_id IS NOT NULL;

-- 2B: Categorize search interactions
CREATE OR REPLACE TEMPORARY VIEW ga4_events_categorized AS
SELECT 
    *,
    -- Classify search submission type from event_label
    CASE 
        WHEN event_label LIKE '%default' THEN 'default_search'
        WHEN event_label LIKE '%auto suggestion' THEN 'auto_suggestion'
        WHEN event_label LIKE '%keyword box' THEN 'keyword_box'
        ELSE 'other'
    END as search_type,
    
    -- Extract click position from result clicks (format: "results|{position}|{url}")
    CASE 
        WHEN event_name = 'global_search_click_result' 
        THEN CAST(SPLIT_PART(event_label, '|', 2) AS INTEGER)
        ELSE NULL
    END as click_position,
    
    -- Flag zero-result searches
    CASE 
        WHEN search_result_count = 0 THEN 1 
        ELSE 0 
    END as is_zero_result,
    
    -- Categorize market by region (example grouping)
    CASE 
        WHEN country_code IN ('US', 'CA') THEN 'North America'
        WHEN country_code IN ('GB', 'DE', 'SE', 'FR', 'IT', 'ES') THEN 'Europe'
        WHEN country_code IN ('CN', 'JP', 'KR') THEN 'Asia'
        ELSE 'Other'
    END as region,
    
    -- Processing metadata
    CURRENT_TIMESTAMP() as etl_processed_at,
    CURRENT_DATE - 1 as data_date
FROM ga4_events_deduped;

-- ============================================================================
-- STEP 3: LOAD - Insert into curated fact table
-- ============================================================================

-- Create curated table if not exists
CREATE TABLE IF NOT EXISTS analytics_db.search_events_fact (
    event_date DATE,
    event_timestamp BIGINT,
    event_name STRING,
    user_pseudo_id STRING,
    search_term STRING,
    search_result_count INT,
    search_type STRING,
    click_position INT,
    is_zero_result INT,
    country_code STRING,
    region STRING,
    device_category STRING,
    traffic_source STRING,
    etl_processed_at TIMESTAMP,
    data_date DATE
)
PARTITIONED BY (data_date)
STORED AS PARQUET;

-- Incremental insert (only insert yesterday's data)
INSERT INTO analytics_db.search_events_fact
SELECT 
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    search_term,
    search_result_count,
    search_type,
    click_position,
    is_zero_result,
    country_code,
    region,
    device_category,
    traffic_source,
    etl_processed_at,
    data_date
FROM ga4_events_categorized
WHERE data_date = CURRENT_DATE - 1;

-- ============================================================================
-- STEP 4: ENRICH - Join with order data from Snowflake
-- ============================================================================

-- Create enriched table with order attribution
CREATE OR REPLACE TABLE analytics_db.search_order_attribution AS
SELECT 
    s.event_date,
    s.user_pseudo_id,
    s.search_term,
    s.search_type,
    s.country_code,
    s.region,
    COUNT(DISTINCT s.event_timestamp) as search_count,
    MAX(CASE WHEN s.event_name = 'global_search_click_result' THEN 1 ELSE 0 END) as clicked_result,
    MAX(s.is_zero_result) as had_zero_result,
    -- Join with order data
    o.order_id,
    o.order_date,
    o.order_value,
    o.product_id,
    -- Calculate days from search to order
    DATEDIFF(o.order_date, s.event_date) as days_to_conversion
FROM analytics_db.search_events_fact s
LEFT JOIN snowflake_db.orders o
    ON s.user_pseudo_id = o.user_pseudo_id
    AND o.order_date BETWEEN s.event_date AND s.event_date + INTERVAL '30 days'
WHERE s.data_date = CURRENT_DATE - 1
GROUP BY 
    s.event_date, s.user_pseudo_id, s.search_term, s.search_type,
    s.country_code, s.region, o.order_id, o.order_date, 
    o.order_value, o.product_id;

-- ============================================================================
-- STEP 5: AGGREGATE - Pre-compute daily metrics for Power BI
-- ============================================================================

CREATE OR REPLACE TABLE analytics_db.search_metrics_daily AS
SELECT 
    event_date,
    region,
    country_code,
    -- Volume metrics
    COUNT(DISTINCT user_pseudo_id) as unique_users,
    COUNT(*) as total_searches,
    -- Engagement metrics
    SUM(clicked_result) as searches_with_clicks,
    ROUND(100.0 * SUM(clicked_result) / COUNT(*), 2) as engagement_rate_pct,
    -- Search quality metrics
    SUM(had_zero_result) as zero_result_searches,
    ROUND(100.0 * SUM(had_zero_result) / COUNT(*), 2) as zero_result_rate_pct,
    -- Search type distribution
    SUM(CASE WHEN search_type = 'default_search' THEN 1 ELSE 0 END) as default_searches,
    SUM(CASE WHEN search_type = 'auto_suggestion' THEN 1 ELSE 0 END) as auto_suggestion_searches,
    SUM(CASE WHEN search_type = 'keyword_box' THEN 1 ELSE 0 END) as keyword_box_searches,
    -- Conversion metrics
    COUNT(DISTINCT CASE WHEN order_id IS NOT NULL THEN user_pseudo_id END) as users_with_orders,
    SUM(order_value) as total_revenue,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN order_id IS NOT NULL THEN user_pseudo_id END) / COUNT(DISTINCT user_pseudo_id), 2) as conversion_rate_pct
FROM analytics_db.search_order_attribution
WHERE event_date = CURRENT_DATE - 1
GROUP BY event_date, region, country_code;

-- ============================================================================
-- STEP 6: DATA QUALITY CHECKS
-- ============================================================================

-- Check 1: Ensure no duplicate events
SELECT 
    'Duplicate Check' as check_name,
    COUNT(*) - COUNT(DISTINCT event_timestamp, user_pseudo_id) as duplicate_count,
    CASE WHEN COUNT(*) = COUNT(DISTINCT event_timestamp, user_pseudo_id) THEN 'PASS' ELSE 'FAIL' END as status
FROM analytics_db.search_events_fact
WHERE data_date = CURRENT_DATE - 1;

-- Check 2: Validate record counts vs source
SELECT 
    'Record Count Check' as check_name,
    COUNT(*) as curated_records,
    (SELECT COUNT(*) FROM ga4_events_raw) as source_records,
    CASE WHEN ABS(COUNT(*) - (SELECT COUNT(*) FROM ga4_events_raw)) < 100 THEN 'PASS' ELSE 'FAIL' END as status
FROM analytics_db.search_events_fact
WHERE data_date = CURRENT_DATE - 1;

-- Check 3: Ensure no NULL critical fields
SELECT 
    'NULL Check' as check_name,
    SUM(CASE WHEN user_pseudo_id IS NULL THEN 1 ELSE 0 END) as null_users,
    SUM(CASE WHEN event_date IS NULL THEN 1 ELSE 0 END) as null_dates,
    CASE WHEN SUM(CASE WHEN user_pseudo_id IS NULL OR event_date IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM analytics_db.search_events_fact
WHERE data_date = CURRENT_DATE - 1;

-- ============================================================================
-- NOTES
-- ============================================================================
-- Pipeline runs daily at 2 AM UTC
-- Processing time: ~5 minutes for 10k events
-- Data available in Power BI by 3 AM UTC
-- Retention: 13 months (curated tables), 3 months (raw GA4)
-- ============================================================================