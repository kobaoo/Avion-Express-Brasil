import psycopg2

# Подключение к базе
conn = psycopg2.connect(
    host="localhost", port="5432",
    dbname="postgres", user="postgres", password="postgres", options='-c search_path=olist'
)

queries = [
    "SELECT DATE_TRUNC('month', order_purchase_timestamp) AS month, COUNT(*) AS total_orders "
    "FROM orders GROUP BY month ORDER BY month LIMIT 5;",

    "SELECT AVG(order_delivered_customer_date - order_purchase_timestamp) AS avg_delivery_time "
    "FROM orders WHERE order_delivered_customer_date IS NOT NULL LIMIT 5;",

    "SELECT customer_city, COUNT(*) AS orders_count "
    "FROM customers JOIN orders USING(customer_id) "
    "GROUP BY customer_city ORDER BY orders_count DESC LIMIT 5;"
]

with conn, conn.cursor() as cur:
    for i, q in enumerate(queries, 1):
        print(f"\n=== Query {i} ===")
        cur.execute(q)
        for row in cur.fetchall():
            print(row)

conn.close()
