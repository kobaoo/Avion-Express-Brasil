import psycopg2
import time
import random
from datetime import datetime, timedelta, date, time as dtime

# --- Параметры генерации ---
ORDER_ID_PREFIX = "bb3b61a129a"  # префикс синтетических заказов для распознавания
SLEEP_BETWEEN_DAYS_SEC = 4       # пауза между днями (для наглядности на дашборде)
BASE_COUNT_MIN = 0               # минимальное количество заказов в первый день
BASE_COUNT_MAX = 10              # максимальное количество заказов в первый день
DAILY_INCREMENT_MAX = 25         # максимум прироста к предыдущему дню
FALLBACK_START_DATE = date(2018, 7, 17)  # если в БД нет ни одной записи

# Режим выбора стартовой даты:
# True  -> стартуем от последнего дня среди ВСЕХ заказов (реальные + синтетические)
# False -> стартуем от последнего дня среди ТОЛЬКО синтетических (по префиксу)
USE_LAST_OF_ALL_ORDERS = True

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port="5432",
        database="postgres",
        user="postgres",
        password="postgres",
        options="-c search_path=olist,public"
    )

def get_dynamic_start_date(cur) -> date:
    """
    Берёт последнюю (максимальную) дату в orders и возвращает её.
    Если записей нет — возвращает FALLBACK_START_DATE.
    При USE_LAST_OF_ALL_ORDERS=False берём последнюю дату только среди синтетических заказов (по префиксу).
    """
    if USE_LAST_OF_ALL_ORDERS:
        cur.execute("SELECT MAX(DATE(order_purchase_timestamp)) FROM orders;")
    else:
        cur.execute("""
            SELECT MAX(DATE(order_purchase_timestamp))
            FROM orders
            WHERE order_id LIKE %s || '%%'
        """, (ORDER_ID_PREFIX,))
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else FALLBACK_START_DATE

def get_last_synthetic_day_and_count(cur, start_date: date):
    """
    Возвращает (last_day_date, count_on_last_day) для СИНТЕТИЧЕСКИХ заказов, начиная с start_date.
    Если их нет — вернёт (start_date - 1 день, 0), чтобы следующий день стал start_date.
    """
    cur.execute("""
        WITH last_day AS (
            SELECT DATE(order_purchase_timestamp) AS d
            FROM orders
            WHERE order_id LIKE %s || '%%'
              AND order_purchase_timestamp >= %s
            ORDER BY d DESC
            LIMIT 1
        )
        SELECT d,
               COALESCE((
                   SELECT COUNT(*)
                   FROM orders
                   WHERE order_id LIKE %s || '%%'
                     AND DATE(order_purchase_timestamp) = d
               ), 0) AS cnt
        FROM last_day
    """, (ORDER_ID_PREFIX, start_date, ORDER_ID_PREFIX))
    row = cur.fetchone()
    if row is None:
        return (start_date - timedelta(days=1), 0)
    return (row[0], row[1])

def random_time_within_day(day: date) -> datetime:
    sec = random.randint(0, 24*60*60 - 1)
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return datetime.combine(day, dtime(hour=h, minute=m, second=s))

def pick_random_ids(cur):
    cur.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1;")
    customer_id = cur.fetchone()[0]

    cur.execute("SELECT seller_id FROM sellers ORDER BY RANDOM() LIMIT 1;")
    seller_id = cur.fetchone()[0]

    cur.execute("SELECT product_id FROM products ORDER BY RANDOM() LIMIT 1;")
    product_id = cur.fetchone()[0]

    return customer_id, seller_id, product_id

def make_order_id():
    return f"{ORDER_ID_PREFIX}{int(datetime.now().timestamp())}_{random.randint(1000,9999)}b10bb81a4770f3b1"

def insert_order_for_timestamp(cur, purchase_dt: datetime):
    customer_id, seller_id, product_id = pick_random_ids(cur)

    order_id = make_order_id()
    estimated_delivery = purchase_dt + timedelta(days=random.randint(10, 45))
    shipping_limit = purchase_dt + timedelta(days=5)

    cur.execute("""
        INSERT INTO orders (
            order_id, customer_id, order_status, order_purchase_timestamp,
            order_estimated_delivery_date
        ) VALUES (%s, %s, 'approved', %s, %s)
    """, (order_id, customer_id, purchase_dt, estimated_delivery))

    price = round(random.uniform(25.0, 450.0), 2)
    freight = round(random.uniform(5.0, 35.0), 2)

    cur.execute("""
        INSERT INTO order_items (
            order_id, order_item_id, product_id, seller_id,
            shipping_limit_date, price, freight_value
        ) VALUES (%s, 1, %s, %s, %s, %s, %s)
    """, (order_id, product_id, seller_id, shipping_limit, price, freight))

    payment_types = ['credit_card', 'boleto', 'voucher', 'debit_card']
    cur.execute("""
        INSERT INTO order_payments (
            order_id, payment_sequential, payment_type,
            payment_installments, payment_value
        ) VALUES (%s, 1, %s, %s, %s)
    """, (
        order_id,
        random.choice(payment_types),
        random.randint(1, 12),
        round(random.uniform(30.0, 500.0), 2)
    ))

def main():
    global DAILY_INCREMENT_MAX
    print("Starting day-by-day backfill with monotonically increasing daily counts...")
    while True:
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            # 1) Динамически определяем стартовую дату
            dynamic_start = get_dynamic_start_date(cur)

            # 2) Узнаём, какой последний СИНТЕТИЧЕСКИЙ день уже заполнен, учитывая dynamic_start
            last_day, last_count = get_last_synthetic_day_and_count(cur, dynamic_start)

            # 3) Следующий день к вставке
            next_day = last_day + timedelta(days=1)

            today = datetime.now().date()
            if next_day > today:
                print(f"Дошли до текущего дня ({today}). Новых дней нет. Ждём...")
                cur.close()
                conn.close()
                time.sleep(5)
                continue

            # 4) Сколько заказов вставлять в next_day
            if last_count == 0 and last_day < dynamic_start:
                # первый «наш» день для этого диапазона
                orders_today = random.randint(BASE_COUNT_MIN, BASE_COUNT_MAX)
            else:
                increment = random.randint(1, DAILY_INCREMENT_MAX)
                if DAILY_INCREMENT_MAX < 400:
                    DAILY_INCREMENT_MAX *= 2
                orders_today = last_count + increment

            print(f"[{next_day}] inserting {orders_today} orders (prev synthetic day had {last_count}); "
                  f"dynamic_start={dynamic_start}")

            # 5) Вставляем заказы, случайно распределяя время в пределах суток
            for _ in range(orders_today):
                purchase_dt = random_time_within_day(next_day)
                insert_order_for_timestamp(cur, purchase_dt)

            conn.commit()
            print(f"[{next_day}] done.")
            cur.close()

            time.sleep(SLEEP_BETWEEN_DAYS_SEC)

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)
        finally:
            if conn:
                conn.close()

if __name__ == "__main__":
    main()
