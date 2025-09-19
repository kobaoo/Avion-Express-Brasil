SET search_path TO olist, public;

-- 1) customers
CREATE TABLE IF NOT EXISTS customers (
  customer_id              TEXT PRIMARY KEY,
  customer_unique_id       TEXT,
  customer_zip_code_prefix INTEGER,
  customer_city            TEXT,
  customer_state           TEXT
);

-- 2) geolocation
CREATE TABLE IF NOT EXISTS geolocation (
  geolocation_zip_code_prefix INTEGER,
  geolocation_lat             NUMERIC(10,6),
  geolocation_lng             NUMERIC(10,6),
  geolocation_city            TEXT,
  geolocation_state           TEXT
);

-- 3) order_items
CREATE TABLE IF NOT EXISTS order_items (
  order_id           TEXT,
  order_item_id      INTEGER,
  product_id         TEXT,
  seller_id          TEXT,
  shipping_limit_date TIMESTAMPTZ,
  price             NUMERIC(10,2),
  freight_value     NUMERIC(10,2),
  PRIMARY KEY (order_id, order_item_id)
);

-- 4) order_payments
CREATE TABLE IF NOT EXISTS order_payments (
  order_id             TEXT,
  payment_sequential   INTEGER,
  payment_type         TEXT,
  payment_installments INTEGER,
  payment_value        NUMERIC(10,2)
);

-- 5) order_reviews
CREATE TABLE IF NOT EXISTS order_reviews (
  review_id               TEXT PRIMARY KEY,
  order_id                TEXT,
  review_score            INTEGER,
  review_comment_title    TEXT,
  review_comment_message  TEXT,
  review_creation_date    TIMESTAMPTZ,
  review_answer_timestamp TIMESTAMPTZ
);

-- 6) orders
CREATE TABLE IF NOT EXISTS orders (
  order_id                       TEXT PRIMARY KEY,
  customer_id                    TEXT,
  order_status                   TEXT,
  order_purchase_timestamp       TIMESTAMPTZ,
  order_approved_at              TIMESTAMPTZ,
  order_delivered_carrier_date   TIMESTAMPTZ,
  order_delivered_customer_date  TIMESTAMPTZ,
  order_estimated_delivery_date  TIMESTAMPTZ
);

-- 7) products
CREATE TABLE IF NOT EXISTS products (
  product_id                 TEXT PRIMARY KEY,
  product_category_name      TEXT,
  product_name_lenght        INTEGER,
  product_description_lenght INTEGER,
  product_photos_qty         INTEGER,
  product_weight_g           INTEGER,
  product_length_cm          INTEGER,
  product_height_cm          INTEGER,
  product_width_cm           INTEGER
);

-- 8) sellers
CREATE TABLE IF NOT EXISTS sellers (
  seller_id              TEXT PRIMARY KEY,
  seller_zip_code_prefix INTEGER,
  seller_city            TEXT,
  seller_state           TEXT
);

-- 9) translation
CREATE TABLE IF NOT EXISTS product_category_name_translation (
  product_category_name         TEXT,
  product_category_name_english TEXT
);

-- Индексы и внешние ключи (необязательно, но полезно)
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_seller_id ON order_items(seller_id);
CREATE INDEX IF NOT EXISTS idx_order_payments_order_id ON order_payments(order_id);
CREATE INDEX IF NOT EXISTS idx_order_reviews_order_id ON order_reviews(order_id);

-- FK добавляем после базовой загрузки, чтобы избежать ошибок при COPY, или оставляем закомментированными:
ALTER TABLE orders       ADD CONSTRAINT fk_orders_customer     FOREIGN KEY (customer_id) REFERENCES customers(customer_id);
ALTER TABLE order_items  ADD CONSTRAINT fk_items_order         FOREIGN KEY (order_id)    REFERENCES orders(order_id);
ALTER TABLE order_items  ADD CONSTRAINT fk_items_product       FOREIGN KEY (product_id)  REFERENCES products(product_id);
ALTER TABLE order_items  ADD CONSTRAINT fk_items_seller        FOREIGN KEY (seller_id)   REFERENCES sellers(seller_id);
ALTER TABLE order_payments ADD CONSTRAINT fk_payments_order    FOREIGN KEY (order_id)    REFERENCES orders(order_id);
ALTER TABLE order_reviews  ADD CONSTRAINT fk_reviews_order     FOREIGN KEY (order_id)    REFERENCES orders(order_id);
