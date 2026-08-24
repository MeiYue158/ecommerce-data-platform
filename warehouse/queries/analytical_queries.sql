-- ════════════════════════════════════════════════════════════
-- ANALYTICAL QUERIES — ClickHouse OLAP Warehouse
-- ════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────
-- REVENUE ANALYSIS
-- ────────────────────────────────────────────────────────────

-- Q1: Monthly revenue trend with MoM growth
SELECT
    d.year_month,
    round(sum(f.price), 2) AS revenue,
    count(DISTINCT f.order_id) AS orders,
    round(sum(f.price) / count(DISTINCT f.order_id), 2) AS aov,
    round((sum(f.price) - lagInFrame(sum(f.price)) OVER (ORDER BY d.year_month))
          / lagInFrame(sum(f.price)) OVER (ORDER BY d.year_month) * 100, 1) AS mom_growth_pct
FROM ecommerce.fact_order_items f
JOIN ecommerce.dim_date d ON f.purchase_date_sk = d.date_sk
GROUP BY d.year_month
ORDER BY d.year_month;

-- Q2: Revenue by category and year
SELECT
    p.category,
    d.year,
    round(sum(f.price), 2) AS revenue,
    count(*) AS items_sold,
    round(avg(f.price), 2) AS avg_item_price
FROM ecommerce.fact_order_items f
JOIN ecommerce.dim_product p ON f.product_sk = p.product_sk
JOIN ecommerce.dim_date d ON f.purchase_date_sk = d.date_sk
WHERE p.category IS NOT NULL
GROUP BY p.category, d.year
ORDER BY d.year, revenue DESC;

-- Q3: Revenue by state with share of total
SELECT
    c.state,
    round(sum(f.price), 2) AS revenue,
    count(DISTINCT f.order_id) AS orders,
    count(DISTINCT c.customer_id) AS customers,
    round(sum(f.price) / (SELECT sum(price) FROM ecommerce.fact_order_items) * 100, 2) AS pct_of_total
FROM ecommerce.fact_order_items f
JOIN ecommerce.dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.state
ORDER BY revenue DESC
LIMIT 10;

-- Q4: Top 10 sellers by revenue
SELECT
    s.seller_id,
    s.city,
    s.state,
    round(sum(f.price), 2) AS revenue,
    count(DISTINCT f.order_id) AS orders,
    count(*) AS items_sold,
    round(avg(f.price), 2) AS avg_price
FROM ecommerce.fact_order_items f
JOIN ecommerce.dim_seller s ON f.seller_sk = s.seller_sk
GROUP BY s.seller_id, s.city, s.state
ORDER BY revenue DESC
LIMIT 10;

-- ────────────────────────────────────────────────────────────
-- CUSTOMER ANALYSIS
-- ────────────────────────────────────────────────────────────

-- Q5: Customer lifetime value (CLV) distribution
SELECT
    multiIf(
        total_spend < 50, 'Under R$50',
        total_spend < 100, 'R$50-100',
        total_spend < 200, 'R$100-200',
        total_spend < 500, 'R$200-500',
        'R$500+'
    ) AS clv_bucket,
    count() AS customers,
    round(avg(total_spend), 2) AS avg_spend,
    round(sum(total_spend), 2) AS total_revenue
FROM (
    SELECT customer_sk, sum(order_value) AS total_spend
    FROM ecommerce.fact_orders
    GROUP BY customer_sk
)
GROUP BY clv_bucket
ORDER BY clv_bucket;

-- Q6: Customer cohort analysis — monthly acquisition cohorts
SELECT
    first_month AS cohort,
    d.year_month AS order_month,
    dateDiff('month', toDate(concat(first_month, '-01')), toDate(concat(d.year_month, '-01'))) AS months_since_first,
    count(DISTINCT fo.customer_sk) AS active_customers,
    round(sum(fo.order_value), 2) AS revenue
FROM ecommerce.fact_orders fo
JOIN ecommerce.dim_date d ON fo.purchase_date_sk = d.date_sk
JOIN (
    SELECT customer_sk, min(year_month) AS first_month
    FROM ecommerce.fact_orders fo2
    JOIN ecommerce.dim_date d2 ON fo2.purchase_date_sk = d2.date_sk
    GROUP BY customer_sk
) cohorts ON fo.customer_sk = cohorts.customer_sk
WHERE first_month >= '2017-01'
GROUP BY cohort, d.year_month
HAVING months_since_first <= 6
ORDER BY cohort, months_since_first;

-- Q7: Repeat purchase rate (customers with >1 order)
SELECT
    total_orders,
    count() AS customers,
    round(count() / (SELECT count(DISTINCT customer_sk) FROM ecommerce.fact_orders) * 100, 2) AS pct
FROM (
    SELECT customer_sk, count() AS total_orders
    FROM ecommerce.fact_orders
    GROUP BY customer_sk
)
GROUP BY total_orders
ORDER BY total_orders
LIMIT 10;

-- ────────────────────────────────────────────────────────────
-- DELIVERY ANALYSIS
-- ────────────────────────────────────────────────────────────

-- Q8: Delivery performance by month
SELECT
    d.year_month,
    count() AS orders,
    round(avg(fo.delivery_days), 1) AS avg_delivery_days,
    round(avg(fo.estimated_delivery_days), 1) AS avg_estimated_days,
    round(countIf(fo.delivery_delay_days > 0) / count() * 100, 1) AS late_delivery_pct
FROM ecommerce.fact_orders fo
JOIN ecommerce.dim_date d ON fo.purchase_date_sk = d.date_sk
WHERE fo.delivery_days > 0
GROUP BY d.year_month
ORDER BY d.year_month;

-- Q9: Delivery delay impact on review scores
SELECT
    multiIf(
        fo.delivery_delay_days <= -7, '7+ Days Early',
        fo.delivery_delay_days <= 0, 'On Time',
        fo.delivery_delay_days <= 7, '1-7 Days Late',
        fo.delivery_delay_days <= 14, '8-14 Days Late',
        '15+ Days Late'
    ) AS delay_bucket,
    count() AS orders,
    round(avg(fr.review_score), 2) AS avg_review,
    round(countIf(fr.review_score = 5) / count() * 100, 1) AS pct_5_star,
    round(countIf(fr.review_score = 1) / count() * 100, 1) AS pct_1_star
FROM ecommerce.fact_orders fo
JOIN ecommerce.fact_reviews fr ON fo.order_id = fr.order_id
WHERE fo.delivery_days > 0
GROUP BY delay_bucket
ORDER BY delay_bucket;

-- Q10: Delivery performance by seller state
SELECT
    s.state AS seller_state,
    count(DISTINCT f.order_id) AS orders,
    round(avg(fo.delivery_days), 1) AS avg_delivery_days,
    round(countIf(fo.delivery_delay_days > 0) / count() * 100, 1) AS late_pct
FROM ecommerce.fact_order_items f
JOIN ecommerce.dim_seller s ON f.seller_sk = s.seller_sk
JOIN ecommerce.fact_orders fo ON f.order_id = fo.order_id
WHERE fo.delivery_days > 0
GROUP BY s.state
HAVING orders >= 100
ORDER BY avg_delivery_days;

-- ────────────────────────────────────────────────────────────
-- PAYMENT ANALYSIS
-- ────────────────────────────────────────────────────────────

-- Q11: Payment type by order value tier
SELECT
    pt.payment_type,
    multiIf(
        fp.payment_value < 50, 'Under R$50',
        fp.payment_value < 100, 'R$50-100',
        fp.payment_value < 200, 'R$100-200',
        fp.payment_value < 500, 'R$200-500',
        'R$500+'
    ) AS value_tier,
    count() AS transactions,
    round(avg(fp.installments), 1) AS avg_installments
FROM ecommerce.fact_payments fp
JOIN ecommerce.dim_payment_type pt ON fp.payment_type_sk = pt.payment_type_sk
GROUP BY pt.payment_type, value_tier
ORDER BY pt.payment_type, value_tier;

-- Q12: Payment behavior by customer state
SELECT
    c.state,
    round(countIf(pt.payment_type = 'credit_card') / count() * 100, 1) AS credit_card_pct,
    round(countIf(pt.payment_type = 'boleto') / count() * 100, 1) AS boleto_pct,
    round(avg(fp.installments), 1) AS avg_installments,
    round(avg(fp.payment_value), 2) AS avg_value
FROM ecommerce.fact_payments fp
JOIN ecommerce.dim_customer c ON fp.customer_sk = c.customer_sk
JOIN ecommerce.dim_payment_type pt ON fp.payment_type_sk = pt.payment_type_sk
GROUP BY c.state
ORDER BY count() DESC
LIMIT 10;

-- ────────────────────────────────────────────────────────────
-- PRODUCT ANALYSIS
-- ────────────────────────────────────────────────────────────

-- Q13: Category performance scorecard
SELECT
    p.category,
    count(DISTINCT f.order_id) AS orders,
    count() AS items_sold,
    round(sum(f.price), 2) AS revenue,
    round(avg(f.price), 2) AS avg_price,
    round(avg(fr.review_score), 2) AS avg_review
FROM ecommerce.fact_order_items f
JOIN ecommerce.dim_product p ON f.product_sk = p.product_sk
LEFT JOIN ecommerce.fact_reviews fr ON f.order_id = fr.order_id
WHERE p.category IS NOT NULL
GROUP BY p.category
ORDER BY revenue DESC
LIMIT 15;

-- Q14: Product weight vs freight cost correlation
SELECT
    multiIf(
        p.weight_g < 500, 'Under 500g',
        p.weight_g < 1000, '500g-1kg',
        p.weight_g < 5000, '1-5kg',
        p.weight_g < 10000, '5-10kg',
        '10kg+'
    ) AS weight_bucket,
    count() AS items,
    round(avg(f.freight_value), 2) AS avg_freight,
    round(avg(f.price), 2) AS avg_price,
    round(avg(f.freight_value) / avg(f.price) * 100, 1) AS freight_pct_of_price
FROM ecommerce.fact_order_items f
JOIN ecommerce.dim_product p ON f.product_sk = p.product_sk
WHERE p.weight_g > 0
GROUP BY weight_bucket
ORDER BY weight_bucket;

-- ────────────────────────────────────────────────────────────
-- SCD TYPE 2 — HISTORICAL ANALYSIS
-- ────────────────────────────────────────────────────────────

-- Q15: Revenue contribution by segment at time of purchase
SELECT
    cs.customer_segment,
    round(sum(fo.order_value), 2) AS revenue,
    count() AS orders,
    round(avg(fo.order_value), 2) AS avg_order_value,
    round(sum(fo.order_value) / (SELECT sum(order_value) FROM ecommerce.fact_orders) * 100, 1) AS pct_revenue
FROM ecommerce.fact_orders_scd2 fo
JOIN ecommerce.dim_customer_scd2 cs ON fo.customer_sk = cs.customer_sk
WHERE cs.customer_segment IS NOT NULL
GROUP BY cs.customer_segment
ORDER BY revenue DESC;

-- Q16: Day-of-week purchase patterns
SELECT
    d.day_name,
    d.day_of_week,
    count(DISTINCT fo.order_id) AS orders,
    round(sum(fo.order_value), 2) AS revenue,
    round(avg(fo.order_value), 2) AS aov
FROM ecommerce.fact_orders fo
JOIN ecommerce.dim_date d ON fo.purchase_date_sk = d.date_sk
GROUP BY d.day_name, d.day_of_week
ORDER BY d.day_of_week;
