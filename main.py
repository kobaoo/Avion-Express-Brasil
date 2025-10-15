import psycopg2
    
conn = psycopg2.connect(
    host="localhost", port="5432",
    dbname="postgres", user="postgres", password="postgres", options='-c search_path=olist'
)

queries = [
    # 1. Кол-во заказов и выручка по месяцам
    "SELECT DATE_TRUNC('month', order_purchase_timestamp) AS month, "
    "COUNT(*) AS total_orders, "
    "SUM(oi.price + oi.freight_value) AS total_revenue "
    "FROM orders o JOIN order_items oi ON o.order_id = oi.order_id "
    "GROUP BY month ORDER BY month;",

    # 2. Среднее время доставки
    "SELECT AVG(order_delivered_customer_date - order_purchase_timestamp) AS avg_delivery_time "
    "FROM orders WHERE order_delivered_customer_date IS NOT NULL;",

    # 3. Топ-10 городов по количеству заказов
    "SELECT c.customer_city, COUNT(o.order_id) AS order_count "
    "FROM orders o JOIN customers c ON o.customer_id = c.customer_id "
    "GROUP BY c.customer_city ORDER BY order_count DESC LIMIT 10;",

    # 4. Категории с наибольшей выручкой
    "SELECT p.product_category_name, SUM(oi.price) AS revenue "
    "FROM order_items oi JOIN products p ON oi.product_id = p.product_id "
    "GROUP BY p.product_category_name ORDER BY revenue DESC LIMIT 10;",

    # 5. Средняя оценка покупателей по категориям
    "SELECT p.product_category_name, AVG(orv.review_score) AS avg_score "
    "FROM order_reviews orv "
    "JOIN orders o ON orv.order_id = o.order_id "
    "JOIN order_items oi ON o.order_id = oi.order_id "
    "JOIN products p ON oi.product_id = p.product_id "
    "GROUP BY p.product_category_name ORDER BY avg_score DESC;",

    # 6. Доля способов оплаты
    "SELECT payment_type, COUNT(*) AS count_payments, "
    "ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS percent_share "
    "FROM order_payments GROUP BY payment_type ORDER BY percent_share DESC;",

    # 7. Средний чек по штатам
    "SELECT c.customer_state, AVG(t.order_total) AS avg_order_total "
    "FROM (SELECT o.order_id, SUM(oi.price + oi.freight_value) AS order_total "
    "      FROM orders o JOIN order_items oi ON o.order_id = oi.order_id "
    "      GROUP BY o.order_id) t "
    "JOIN orders o ON t.order_id = o.order_id "
    "JOIN customers c ON o.customer_id = c.customer_id "
    "GROUP BY c.customer_state ORDER BY avg_order_total DESC;",

    # 8. Продавцы с наибольшим количеством проданных товаров
    "SELECT s.seller_id, COUNT(*) AS items_sold "
    "FROM order_items oi JOIN sellers s ON oi.seller_id = s.seller_id "
    "GROUP BY s.seller_id ORDER BY items_sold DESC LIMIT 10;",

    # 9. Процент заказов, доставленных позже обещанного срока
    "SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE order_delivered_customer_date > order_estimated_delivery_date) "
    "/ COUNT(*), 2) AS late_delivery_percent "
    "FROM orders WHERE order_delivered_customer_date IS NOT NULL;",

    # 10. Самые популярные категории товаров по количеству позиций
    "SELECT p.product_category_name, COUNT(*) AS items_count "
    "FROM order_items oi JOIN products p ON oi.product_id = p.product_id "
    "GROUP BY p.product_category_name ORDER BY items_count DESC LIMIT 10;"
]

with conn, conn.cursor() as cur:
    for i, q in enumerate(queries, 1):
        print(f"\n=== Query {i} ===")
        cur.execute(q)
        for row in cur.fetchall():
            print(row)

conn.close()
