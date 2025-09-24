import os
from typing import Optional, List, Any, Dict
from dataclasses import dataclass
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, constr, conint
from psycopg2.pool import SimpleConnectionPool
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os, time
import psycopg2
from psycopg2.pool import SimpleConnectionPool

def wait_pg_and_get_pool(max_attempts=20, delay=1.5):
    dsn = (
        f"host={os.getenv('PGHOST','db')} "
        f"port={os.getenv('PGPORT','5432')} "
        f"dbname={os.getenv('PGDATABASE','postgres')} "
        f"user={os.getenv('PGUSER','postgres')} "
        f"password={os.getenv('PGPASSWORD','postgres')}"
    )
    last_err = None
    for i in range(1, max_attempts+1):
        try:
            pool = SimpleConnectionPool(minconn=1, maxconn=10, dsn=dsn)
            # проверим соединение сразу
            conn = pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    cur.fetchone()
            finally:
                pool.putconn(conn)
            print(f"[startup] Connected to Postgres on attempt {i}")
            return pool
        except Exception as e:
            last_err = e
            print(f"[startup] Postgres not ready (attempt {i}/{max_attempts}): {e}")
            time.sleep(delay)
    raise RuntimeError(f"Cannot connect to Postgres after {max_attempts} attempts: {last_err}")


load_dotenv()

PGHOST   = os.getenv("PGHOST", "localhost")
PGPORT   = int(os.getenv("PGPORT", "5432"))
PGDB     = os.getenv("PGDATABASE", "postgres")
PGUSER   = os.getenv("PGUSER", "postgres")
PGPASS   = os.getenv("PGPASSWORD", "postgres")
PGSCHEMA = os.getenv("PGSCHEMA", "olist")

# ---------- Pydantic schemas ----------
class CustomerIn(BaseModel):
    customer_id: str
    customer_unique_id: Optional[str] = None
    customer_zip_code_prefix: conint(ge=0) = 0
    customer_city: str = "unknown"
    customer_state: constr(strip_whitespace=True, min_length=1, max_length=5) = "NA"

class SellerIn(BaseModel):
    seller_id: str
    seller_zip_code_prefix: conint(ge=0) = 0
    seller_city: str = "unknown"
    seller_state: constr(strip_whitespace=True, min_length=1, max_length=5) = "NA"

class OrderIn(BaseModel):
    order_id: str
    customer_id: str
    order_status: str = "created"

# ---------- DB pool + helpers ----------
pool: Optional[SimpleConnectionPool] = None

def _get_conn():
    if pool is None:
        raise RuntimeError("DB pool not initialized")
    conn = pool.getconn()
    with conn.cursor() as cur:
        cur.execute("SET search_path TO %s, public;", (PGSCHEMA,))
    return conn

def _put_conn(conn):
    if pool:
        pool.putconn(conn)

app = FastAPI(title="Olist API", version="1.0.0")

@app.on_event("startup")
def startup():
    global pool
    pool = wait_pg_and_get_pool()
    pool = SimpleConnectionPool(
        minconn=1, maxconn=10,
        dsn=f"host={PGHOST} port={PGPORT} dbname={PGDB} user={PGUSER} password={PGPASS}",
        cursor_factory=RealDictCursor,
    )

@app.on_event("shutdown")
def shutdown():
    global pool
    if pool:
        pool.closeall()
        pool = None

# ---------- Endpoints ----------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/customers", status_code=201)
def create_customer(body: CustomerIn):
    conn = _get_conn()
    try:
        cuid = body.customer_unique_id or body.customer_id
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO customers
                      (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO NOTHING;
                """, (body.customer_id, cuid, body.customer_zip_code_prefix, body.customer_city, body.customer_state))
        return {"ok": True, "customer_id": body.customer_id}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        _put_conn(conn)

@app.post("/sellers", status_code=201)
def create_seller(body: SellerIn):
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sellers
                      (seller_id, seller_zip_code_prefix, seller_city, seller_state)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (seller_id) DO NOTHING;
                """, (body.seller_id, body.seller_zip_code_prefix, body.seller_city, body.seller_state))
        return {"ok": True, "seller_id": body.seller_id}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        _put_conn(conn)

@app.post("/orders", status_code=201)
def create_order(body: OrderIn):
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO orders (
                      order_id, customer_id, order_status,
                      order_purchase_timestamp, order_approved_at,
                      order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
                    )
                    VALUES (%s, %s, %s, NULL, NULL, NULL, NULL, NULL)
                    ON CONFLICT (order_id) DO NOTHING;
                """, (body.order_id, body.customer_id, body.order_status))
        return {"ok": True, "order_id": body.order_id}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        _put_conn(conn)

@app.get("/orders/by-customer-id/{customer_id}")
def orders_by_customer_id(customer_id: str):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  o.order_id, o.order_status,
                  o.order_purchase_timestamp, o.order_delivered_customer_date,
                  c.customer_id, c.customer_unique_id, c.customer_city, c.customer_state
                FROM orders o
                JOIN customers c ON c.customer_id = o.customer_id
                WHERE c.customer_id = %s
                ORDER BY o.order_purchase_timestamp DESC NULLS LAST;
            """, (customer_id,))
            rows = cur.fetchall()
        if not rows:
            raise HTTPException(404, "orders not found")
        return rows
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        _put_conn(conn)

@app.get("/orders/by-city")
def orders_by_city(city: str = Query(..., description="Exact match (case-insensitive)")):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            # ILIKE без % — точное (но case-insensitive) совпадение.
            cur.execute("""
                SELECT
                  o.order_id, o.order_status,
                  o.order_purchase_timestamp, o.order_delivered_customer_date,
                  c.customer_id, c.customer_unique_id, c.customer_city, c.customer_state
                FROM orders o
                JOIN customers c ON c.customer_id = o.customer_id
                WHERE c.customer_city ILIKE %s
                ORDER BY o.order_purchase_timestamp DESC NULLS LAST;
            """, (city,))
            rows = cur.fetchall()
        if not rows:
            raise HTTPException(404, "orders not found")
        return rows
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        _put_conn(conn)
