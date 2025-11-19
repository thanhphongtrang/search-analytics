-- ============================================================================
-- BEHAVIORAL METRICS FOR SEARCH PRODUCT ANALYTICS
-- ============================================================================
-- Business Context:
-- Tracking customer search behavior across 100+ markets to optimize
-- search relevance and drive online order completion.
--
-- Data Sources: GA4 (behavioral events), Snowflake (order data)
-- Key Metrics: Engagement rate, conversion rate, zero-result searches
-- ============================================================================

-- ============================================================================
-- METRIC 1: SEARCH ENGAGEMENT RATE
-- ============================================================================
-- Engagement Rate = (Sessions with result clicks) / (Sessions with searches)
-- This is a KEY metric showing how often users find relevant results

WITH search_sessions AS (
    -- Count sessions that submitted a search
    SELECT 
        event_date,
        user_pseudo_id,
        COUNT(DISTINCT CASE WHEN event_name = 'global_search_submit' THEN event_timestamp END) as search_count
    FROM ga4_events
    WHERE event_category = 'global search'
        AND event_name = 'global_search_submit'
    GROUP BY event_date, user_pseudo_id
),

engagement_sessions AS (
    -- Count sessions that clicked on search results
    SELECT 
        event_date,
        user_pseudo_id,
        COUNT(DISTINCT CASE WHEN event_name = 'global_search_click_result' THEN event_timestamp END) as click_count
    FROM ga4_events
    WHERE event_category = 'global search'
        AND event_name = 'global_search_click_result'
    GROUP BY event_date, user_pseudo_id
)

SELECT 
    s.event_date,
    COUNT(DISTINCT s.user_pseudo_id) as total_search_sessions,
    COUNT(DISTINCT e.user_pseudo_id) as engaged_sessions,
    ROUND(100.0 * COUNT(DISTINCT e.user_pseudo_id) / COUNT(DISTINCT s.user_pseudo_id), 2) as engagement_rate_pct
FROM search_sessions s
LEFT JOIN engagement_sessions e
    ON s.event_date = e.event_date
    AND s.user_pseudo_id = e.user_pseudo_id
GROUP BY s.event_date
ORDER BY s.event_date DESC;

-- ============================================================================
-- METRIC 2: ZERO-RESULT SEARCHES (Content Gap Analysis)
-- ============================================================================
-- Identifies search terms that return zero results
-- These represent content gaps and opportunities for improvement

SELECT 
    search_term,
    COUNT(*) as zero_result_count,
    COUNT(DISTINCT user_pseudo_id) as affected_users,
    COUNT(DISTINCT event_date) as days_occurring
FROM ga4_events
WHERE event_category = 'global search'
    AND event_name = 'page_view'
    AND page_name = 'search result page'
    AND search_result_count = 0
    AND event_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY search_term
ORDER BY zero_result_count DESC
LIMIT 50;

-- ============================================================================
-- METRIC 3: SEARCH-TO-ORDER CONVERSION FUNNEL
-- ============================================================================
-- Tracks the complete journey from search to order completion
-- Key business impact metric showing revenue attribution

WITH search_users AS (
    -- Users who performed searches in the analysis period
    SELECT DISTINCT
        user_pseudo_id,
        MIN(event_date) as first_search_date
    FROM ga4_events
    WHERE event_category = 'global search'
        AND event_name = 'global_search_submit'
        AND event_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY user_pseudo_id
),

order_users AS (
    -- Users who completed orders in the analysis period
    SELECT DISTINCT
        user_pseudo_id,
        COUNT(DISTINCT order_id) as order_count,
        SUM(order_value) as total_revenue
    FROM orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY user_pseudo_id
)

SELECT 
    COUNT(DISTINCT s.user_pseudo_id) as total_search_users,
    COUNT(DISTINCT o.user_pseudo_id) as users_with_orders,
    ROUND(100.0 * COUNT(DISTINCT o.user_pseudo_id) / COUNT(DISTINCT s.user_pseudo_id), 2) as conversion_rate_pct,
    SUM(o.order_count) as total_orders,
    ROUND(AVG(o.total_revenue), 2) as avg_revenue_per_converting_user
FROM search_users s
LEFT JOIN order_users o
    ON s.user_pseudo_id = o.user_pseudo_id;

-- ============================================================================
-- METRIC 4: CLICK POSITION ANALYSIS
-- ============================================================================
-- Analyzes which result positions users click most often
-- Helps optimize result ranking and layout

SELECT 
    event_label,
    -- Extract position from event_label format: "results|{position}|{url}"
    CAST(SPLIT_PART(event_label, '|', 2) AS INTEGER) as click_position,
    COUNT(*) as click_count,
    COUNT(DISTINCT user_pseudo_id) as unique_users,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as click_share_pct
FROM ga4_events
WHERE event_category = 'global search'
    AND event_name = 'global_search_click_result'
    AND event_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY event_label, click_position
ORDER BY click_position ASC;

-- ============================================================================
-- METRIC 5: SEARCH REFINEMENT RATE
-- ============================================================================
-- Measures how often users modify their search
-- High refinement rate suggests initial results weren't relevant

WITH user_search_sessions AS (
    SELECT 
        user_pseudo_id,
        event_date,
        COUNT(*) as search_count,
        COUNT(DISTINCT search_term) as unique_terms
    FROM ga4_events
    WHERE event_category = 'global search'
        AND event_name = 'global_search_submit'
    GROUP BY user_pseudo_id, event_date
)

SELECT 
    event_date,
    COUNT(*) as total_sessions,
    SUM(CASE WHEN search_count > 1 THEN 1 ELSE 0 END) as sessions_with_refinement,
    ROUND(100.0 * SUM(CASE WHEN search_count > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as refinement_rate_pct,
    ROUND(AVG(search_count), 2) as avg_searches_per_session
FROM user_search_sessions
GROUP BY event_date
ORDER BY event_date DESC;

-- ============================================================================
-- NOTES
-- ============================================================================
-- These metrics formed the foundation of the executive dashboard
-- Engagement rate target: >60% (achieved 65% post-optimization)
-- Refinement rate target: <25% (achieved 30% reduction)
-- Business impact: ~10% improvement in online order completion rate
-- ============================================================================