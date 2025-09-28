-- All queries used by analytics.py (you can run them manually to preview results)

-- 1) Line chart: monthly revenue trend
WITH order_totals AS (
  SELECT o.order_id,
         DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
         SUM(oi.price + oi.freight_value) AS order_total
  FROM orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  GROUP BY o.order_id, DATE_TRUNC('month', o.order_purchase_timestamp)
)
SELECT month, SUM(order_total) AS monthly_revenue
FROM order_totals
GROUP BY month
ORDER BY month;

-- 2) Pie chart: payment method share (by count of payments)
SELECT op.payment_type,
       COUNT(*) AS cnt
FROM order_payments op
JOIN orders o ON op.order_id = o.order_id
JOIN order_items oi ON o.order_id = oi.order_id  -- ensure multi-table join
GROUP BY op.payment_type
ORDER BY cnt DESC;

-- 3) Bar chart: top-10 categories by revenue (with English names if available)
SELECT COALESCE(t.product_category_name_english, p.product_category_name) AS category,
       ROUND(SUM(oi.price), 2) AS revenue
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
LEFT JOIN product_category_name_translation t
  ON t.product_category_name = p.product_category_name
GROUP BY COALESCE(t.product_category_name_english, p.product_category_name)
ORDER BY revenue DESC
LIMIT 10;

-- 4) Horizontal bar: avg review score by category (top-10 by count to be robust)
WITH scored AS (
  SELECT COALESCE(t.product_category_name_english, p.product_category_name) AS category,
         orv.review_score
  FROM order_reviews orv
  JOIN orders o ON orv.order_id = o.order_id
  JOIN order_items oi ON o.order_id = oi.order_id
  JOIN products p ON oi.product_id = p.product_id
  LEFT JOIN product_category_name_translation t
    ON t.product_category_name = p.product_category_name
  WHERE orv.review_score IS NOT NULL
)
SELECT category,
       ROUND(AVG(review_score)::NUMERIC, 3) AS avg_score,
       COUNT(*) AS n_reviews
FROM scored
GROUP BY category
HAVING COUNT(*) >= 50 -- filter low-sample noise
ORDER BY avg_score DESC, n_reviews DESC
LIMIT 10;

-- 5) Histogram: distribution of per-order totals
WITH per_order AS (
  SELECT o.order_id,
         SUM(oi.price + oi.freight_value) AS order_total
  FROM orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  JOIN order_payments op ON op.order_id = o.order_id -- ensure 2+ joins
  GROUP BY o.order_id
)
SELECT * FROM per_order;

-- 6) Scatter: delivery time (days) vs order total (only delivered & paid orders)
WITH per_order AS (
  SELECT o.order_id,
         EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400.0 AS delivery_days,
         SUM(oi.price + oi.freight_value) AS order_total
  FROM orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  JOIN order_payments op ON op.order_id = o.order_id
  WHERE o.order_delivered_customer_date IS NOT NULL
  GROUP BY o.order_id, delivery_days
)
SELECT * FROM per_order
WHERE delivery_days BETWEEN 0 AND 60; -- trim outliers for readability

-- 7) Extra (used in Excel export): avg order total by state
WITH per_order AS (
  SELECT o.order_id,
         SUM(oi.price + oi.freight_value) AS order_total
  FROM orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  GROUP BY o.order_id
)
SELECT c.customer_state,
       ROUND(AVG(p.order_total), 2) AS avg_order_total,
       COUNT(*) AS n_orders
FROM per_order p
JOIN orders o ON p.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY avg_order_total DESC;
