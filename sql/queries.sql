-- =====================================================================
-- RETAIL ANALYTICS SQL SUITE
-- Business Use Case Structured for an End-to-End Analytics Project
-- Covers: Sales, Product, Store/Region, Customer, Cohorts, RFM, KPIs
-- =====================================================================


/* =====================================================================
   SECTION 1: SALES OVERVIEW ANALYTICS
   - Total revenue
   - Monthly trends
   - Growth analysis
   - Running cumulative totals
===================================================================== */

-- 1.1 Total Sales Revenue
SELECT SUM(total_amount) AS total_revenue
FROM sales_fact;

-- 1.2 Monthly Sales Trend (Revenue by Month)
SELECT
    DATE_TRUNC('month', sale_date) AS sale_month,
    SUM(total_amount) AS monthly_revenue
FROM sales_fact
GROUP BY sale_month
ORDER BY sale_month;

-- 1.3 Monthly Revenue Growth Rate
SELECT
    sale_month,
    monthly_revenue,
    LAG(monthly_revenue) OVER (ORDER BY sale_month) AS prev_month_revenue,
    ROUND(
        (
            (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY sale_month)) /
            NULLIF(LAG(monthly_revenue) OVER (ORDER BY sale_month), 0)
        )::numeric,
    2) AS growth_pct
FROM (
    SELECT
        DATE_TRUNC('month', sale_date) AS sale_month,
        SUM(total_amount) AS monthly_revenue
    FROM sales_fact
    GROUP BY sale_month
) m
ORDER BY sale_month;

-- 1.4 Running Total Revenue by Region (Cumulative)
SELECT
    s.sale_date,
    st.region,
    SUM(s.total_amount) AS daily_sales,
    SUM(SUM(s.total_amount)) OVER (
        PARTITION BY st.region
        ORDER BY s.sale_date
        ROWS UNBOUNDED PRECEDING
    ) AS running_total_sales
FROM sales_fact s
JOIN dim_store st ON s.store_id = st.store_id
GROUP BY s.sale_date, st.region
ORDER BY st.region, s.sale_date;



/* =====================================================================
   SECTION 2: PRODUCT ANALYTICS
   - Best-sellers
   - Product rankings by store
   - Category performance
===================================================================== */

-- 2.1 Top 5 Best-Selling Products (by Quantity)
SELECT 
    p.product_name,
    SUM(s.quantity) AS total_quantity_sold
FROM sales_fact s
JOIN dim_product p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_quantity_sold DESC
LIMIT 5;

-- 2.2 Top 5 Products by Revenue
SELECT
    p.product_name,
    SUM(s.total_amount) AS total_revenue
FROM sales_fact s
JOIN dim_product p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 5;


-- 2.3 Top 3 Products by Revenue per Store (Window Function)
WITH RankedProducts AS (
    SELECT
        st.store_name,
        p.product_name,
        SUM(s.total_amount) AS product_revenue,
        RANK() OVER (PARTITION BY st.store_name ORDER BY SUM(s.total_amount) DESC) AS rank
    FROM sales_fact s
    JOIN dim_store st ON s.store_id = st.store_id
    JOIN dim_product p ON s.product_id = p.product_id
    GROUP BY st.store_name, p.product_name
)
SELECT store_name, product_name, product_revenue
FROM RankedProducts
WHERE rank <= 3
ORDER BY store_name, rank;

-- 2.4 Category Performance with Subtotals (ROLLUP)
SELECT
    COALESCE(p.category, 'ALL CATEGORIES') AS category,
    COALESCE(st.store_name, 'ALL STORES') AS store_name,
    SUM(s.quantity) AS total_quantity_sold,
    ROUND(SUM(s.total_amount)::numeric, 2) AS total_revenue
FROM sales_fact s
JOIN dim_product p ON s.product_id = p.product_id
JOIN dim_store st ON s.store_id = st.store_id
GROUP BY ROLLUP(p.category, st.store_name)
ORDER BY p.category NULLS LAST, st.store_name NULLS LAST;



/* =====================================================================
   SECTION 3: STORE & REGION ANALYTICS
   - Monthly region revenue
   - Store-level order economics
===================================================================== */

-- 3.1 Monthly Sales by Region
SELECT
    DATE_TRUNC('month', s.sale_date) AS sale_month,
    st.region,
    SUM(s.total_amount) AS monthly_revenue
FROM sales_fact s
JOIN dim_store st ON s.store_id = st.store_id
GROUP BY sale_month, st.region
ORDER BY sale_month, st.region;

-- 3.2 Average Order Value (AOV) by Store
SELECT
    st.store_name,
    ROUND(AVG(s.total_amount)::numeric, 2) AS avg_order_value
FROM sales_fact s
JOIN dim_store st ON s.store_id = st.store_id
GROUP BY st.store_name
ORDER BY avg_order_value DESC;



/* =====================================================================
   SECTION 4: CUSTOMER ANALYTICS
   - Purchase frequency
   - High-value customers
===================================================================== */

-- 4.1 Customer Purchase Summary (Frequent Buyers)
WITH CustomerPurchases AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        s.total_amount
    FROM sales_fact s
    JOIN dim_customer c ON s.customer_id = c.customer_id
)
SELECT
    customer_id,
    first_name,
    last_name,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS total_spent
FROM CustomerPurchases
GROUP BY customer_id, first_name, last_name
HAVING COUNT(*) >= 3
ORDER BY total_spent DESC
LIMIT 10;



/* =====================================================================
   SECTION 5: COHORT RETENTION ANALYTICS
   - Signup cohorts
   - Period-based retention calculation
===================================================================== */

WITH CustomerCohort AS (
    SELECT customer_id, DATE_TRUNC('month', signup_date) AS cohort
    FROM dim_customer
),
SalesWithCohort AS (
    SELECT
        s.customer_id,
        cc.cohort,
        DATE_TRUNC('month', s.sale_date) AS order_month
    FROM sales_fact s
    JOIN CustomerCohort cc ON s.customer_id = cc.customer_id
),
CohortSize AS (
    SELECT cohort, COUNT(DISTINCT customer_id) AS cohort_size
    FROM CustomerCohort
    GROUP BY cohort
),
Retention AS (
    SELECT
        swc.cohort,
        cs.cohort_size,
        (EXTRACT(YEAR FROM swc.order_month) - EXTRACT(YEAR FROM swc.cohort)) * 12 +
        (EXTRACT(MONTH FROM swc.order_month) - EXTRACT(MONTH FROM swc.cohort)) AS period,
        COUNT(DISTINCT swc.customer_id) AS active_customers
    FROM SalesWithCohort swc
    JOIN CohortSize cs ON swc.cohort = cs.cohort
    GROUP BY swc.cohort, cs.cohort_size, period
)
SELECT
    cohort,
    cohort_size,
    MAX(CASE WHEN period = 0 THEN active_customers END) AS m0_customers,
    MAX(CASE WHEN period = 1 THEN active_customers END) AS m1_customers,
    MAX(CASE WHEN period = 2 THEN active_customers END) AS m2_customers,
    ROUND(
        (MAX(CASE WHEN period = 1 THEN active_customers END)::numeric /
         NULLIF(MAX(CASE WHEN period = 0 THEN active_customers END), 0)) * 100, 
    2) AS m1_retention_pct
FROM Retention
GROUP BY cohort, cohort_size
ORDER BY cohort;



/* =====================================================================
   SECTION 6: RFM CUSTOMER SEGMENTATION
   - RFM metrics
   - NTILE ranking
   - Segmentation labels
===================================================================== */

WITH CustomerMetrics AS (
    SELECT
        customer_id,
        CURRENT_DATE - MAX(sale_date) AS recency_days,
        COUNT(*) AS frequency,
        SUM(total_amount) AS monetary
    FROM sales_fact
    GROUP BY customer_id
),
RFMScores AS (
    SELECT
        customer_id,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM CustomerMetrics
)
SELECT
    cm.customer_id,
    d.first_name,
    d.last_name,
    cm.recency_days,
    cm.frequency,
    cm.monetary,
    r.r_score,
    r.f_score,
    r.m_score,
    CASE
        WHEN r.r_score >= 4 AND r.f_score >= 4 AND r.m_score >= 4 THEN 'Champion'
        WHEN r.r_score >= 3 AND r.f_score >= 3 AND r.m_score >= 3 THEN 'Loyal'
        WHEN r.r_score >= 4 AND r.f_score <= 2 AND r.m_score <= 2 THEN 'New'
        WHEN r.r_score <= 2 AND r.f_score >= 3 AND r.m_score >= 3 THEN 'At Risk'
        ELSE 'Others'
    END AS segment
FROM CustomerMetrics cm
JOIN RFMScores r ON cm.customer_id = r.customer_id
JOIN dim_customer d ON cm.customer_id = d.customer_id
ORDER BY cm.monetary DESC
LIMIT 20;



/* =====================================================================
   SECTION 7: KPI DASHBOARD – TARGET vs ACTUAL
   - Variance analysis
   - Achievement %
===================================================================== */

WITH MonthlyTargets AS (
    SELECT 'North' AS region, 50000 AS monthly_target
    UNION ALL SELECT 'South', 45000
    UNION ALL SELECT 'East', 60000
    UNION ALL SELECT 'West', 55000
),
ActualSales AS (
    SELECT
        st.region,
        SUM(s.total_amount) AS actual_sales
    FROM sales_fact s
    JOIN dim_store st ON s.store_id = st.store_id
    WHERE s.sale_date >= '2023-01-01' AND s.sale_date < '2023-02-01'
    GROUP BY st.region
)
SELECT
    mt.region,
    mt.monthly_target,
    COALESCE(a.actual_sales, 0) AS actual_sales,
    (COALESCE(a.actual_sales, 0) - mt.monthly_target) AS variance,
    ROUND(
        (COALESCE(a.actual_sales, 0)::numeric / mt.monthly_target::numeric) * 100,
    2) AS achievement_pct
FROM MonthlyTargets mt
LEFT JOIN ActualSales a ON mt.region = a.region
ORDER BY mt.region;
