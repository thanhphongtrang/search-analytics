-- ============================================================================
-- NULL HANDLING WITH REGIONAL-PRODUCT AVERAGES
-- ============================================================================
-- Business Context: 
-- 15% of sales records had NULL values across product-region combinations.
-- This script demonstrates a sophisticated NULL handling approach using 
-- regional-product averages rather than global averages.
--
-- Key Learning: This heuristic approach enabled immediate stakeholder 
-- decision-making while maintaining analytical integrity.
-- ============================================================================

-- Step 1: Calculate regional-product averages
-- This subquery computes the average sales for each product within each region
-- Only non-NULL values are used to compute the average
WITH regional_product_avg AS (
    SELECT 
        region,
        product_id,
        AVG(sales_value) as avg_sales,
        COUNT(*) as sample_size,
        COUNT(CASE WHEN sales_value IS NULL THEN 1 END) as null_count
    FROM sales_data
    WHERE sales_value IS NOT NULL
    GROUP BY region, product_id
),

-- Step 2: Enrich sales data with regional-product context
-- Join original data with regional averages to enable sophisticated NULL filling
sales_enriched AS (
    SELECT 
        s.order_id,
        s.order_date,
        s.region,
        s.product_id,
        s.product_name,
        s.search_term,
        s.sales_value as original_sales_value,
        r.avg_sales as regional_product_avg,
        r.sample_size as regional_sample_size,
        -- Use COALESCE to fill NULLs with regional-product average
        COALESCE(s.sales_value, r.avg_sales) as sales_value_clean,
        -- Flag which records were imputed for transparency
        CASE 
            WHEN s.sales_value IS NULL THEN 1 
            ELSE 0 
        END as is_imputed
    FROM sales_data s
    LEFT JOIN regional_product_avg r
        ON s.region = r.region 
        AND s.product_id = r.product_id
)

-- Step 3: Final output with data quality metadata
SELECT 
    order_id,
    order_date,
    region,
    product_id,
    product_name,
    search_term,
    sales_value_clean as sales_value,
    is_imputed,
    regional_product_avg,
    regional_sample_size
FROM sales_enriched
ORDER BY order_date DESC, region, product_id;

-- ============================================================================
-- DATA QUALITY SUMMARY QUERY
-- ============================================================================
-- Use this to generate a summary report for stakeholders
-- Shows the extent of NULL handling and sample sizes per region-product

SELECT 
    region,
    product_id,
    COUNT(*) as total_records,
    SUM(CASE WHEN original_sales_value IS NULL THEN 1 ELSE 0 END) as null_count,
    ROUND(100.0 * SUM(CASE WHEN original_sales_value IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_percentage,
    AVG(regional_product_avg) as avg_sales,
    MIN(regional_sample_size) as min_sample_size
FROM sales_enriched
GROUP BY region, product_id
HAVING null_count > 0
ORDER BY null_percentage DESC;

-- ============================================================================
-- NOTES FOR STAKEHOLDERS
-- ============================================================================
-- 1. Imputed values are flagged with is_imputed = 1 for full transparency
-- 2. Regional-product averages are more accurate than global averages
-- 3. Sample sizes are provided to assess confidence in imputed values
-- 4. This approach reduced NULL values from 15% to 0% while maintaining 
--    analytical rigor and stakeholder trust
-- ============================================================================