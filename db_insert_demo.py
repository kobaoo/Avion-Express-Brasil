import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDB   = os.getenv("PGDATABASE", "postgres")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASS = os.getenv("PGPASSWORD", "postgres")
PGSCHEMA = os.getenv("PGSCHEMA", "olist")

def get_conn():
    dsn = f"host={PGHOST} port={PGPORT} dbname={PGDB} user={PGUSER} password={PGPASS}"
    conn = psycopg2.connect(dsn)
    conn.autocommit = False  # управляем транзакциями сами
    with conn.cursor() as cur:
        cur.execute("SET search_path TO %s, public;", (PGSCHEMA,))
    return conn

def insert_customer(conn, customer):
    """
    customer: (customer_id, customer_unique_id, zip_prefix, city, state)
    """
    sql = """
    INSERT INTO customers (
        customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, customer)

def insert_seller(conn, seller):
    """
    seller: (seller_id, zip_prefix, city, state)
    """
    sql = """
    INSERT INTO sellers (
        seller_id, seller_zip_code_prefix, seller_city, seller_state
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (seller_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, seller)

def insert_order(conn, order):
    """
    order: (order_id, customer_id, order_status)
    Остальные даты можно оставить NULL.
    """
    sql = """
    INSERT INTO orders (
        order_id, customer_id, order_status,
        order_purchase_timestamp, order_approved_at,
        order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
    )
    VALUES (%s, %s, %s, NULL, NULL, NULL, NULL, NULL)
    ON CONFLICT (order_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, order)

def interactive():
    print("=== PostgreSQL demo insert (olist) ===")
    try:
        conn = get_conn()
    except Exception as e:
        print("❌ Не удалось подключиться к БД:", e)
        sys.exit(1)

    try:
        # 1) Вставим покупателя (customers)
        print("\n— Добавим customer —")
        cid = input("customer_id (строка, уникальная): ").strip()
        cuid = input("customer_unique_id (можно тот же или иной): ").strip() or cid
        zip_prefix = input("customer_zip_code_prefix (число): ").strip() or "0"
        city = input("customer_city: ").strip() or "unknown"
        state = input("customer_state (две буквы, например SP): ").strip() or "NA"

        insert_customer(conn, (cid, cuid, int(zip_prefix), city, state))
        print("✅ customers: вставка/апсерт выполнены")

        # 2) Вставим продавца (sellers)
        print("\n— Добавим seller —")
        sid = input("seller_id (строка, уникальная): ").strip()
        szip = input("seller_zip_code_prefix (число): ").strip() or "0"
        scity = input("seller_city: ").strip() or "unknown"
        sstate = input("seller_state: ").strip() or "NA"

        insert_seller(conn, (sid, int(szip), scity, sstate))
        print("✅ sellers: вставка/апсерт выполнены")

        # 3) (опционально) Вставим пустой заказ, привязанный к customer
        print("\n— Добавим order (опционально) —")
        do_order = input("Создать тестовый order? [y/N]: ").strip().lower()
        if do_order == "y":
            oid = input("order_id (строка, уникальная): ").strip()
            status = input("order_status (например created/shipped/delivered): ").strip() or "created"
            insert_order(conn, (oid, cid, status))
            print("✅ orders: вставка/апсерт выполнены")

        conn.commit()
        print("\n🎉 Готово! Транзакция зафиксирована.")
    except Exception as e:
        conn.rollback()
        print("⛔ Ошибка, транзакция откатена:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    interactive()
