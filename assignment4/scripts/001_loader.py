#!/usr/bin/env python3
import psycopg2
import time
import random
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

def create_connection():
    conn = psycopg2.connect(
        host="localhost",
        port="5432", 
        database="postgres",
        user="postgres",
        password="postgres"
    )
    # Устанавливаем схему по умолчанию
    cursor = conn.cursor()
    cursor.execute("SET search_path TO olist, public;")
    return conn

def generate_customers():
    """Генерация тестовых клиентов"""
    conn = create_connection()
    cursor = conn.cursor()
    
    customers_data = []
    for i in range(500):
        customer_id = f"load_cust_{i:05d}"
        customers_data.append(( 
            customer_id,
            f"unique_{i:05d}",
            random.randint(1000, 99999),
            random.choice(['Sao Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Porto Alegre', 'Salvador', 'Brasilia', 'Fortaleza', 'Recife', 'Curitiba', 'Manaus']),
            random.choice(['SP', 'RJ', 'MG', 'RS', 'BA', 'DF', 'CE', 'PE', 'PR', 'AM'])
        ))
        
        # Коммит каждые 50 записей
        if len(customers_data) % 50 == 0:
            try:
                cursor.executemany("""
                    INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO NOTHING
                """, customers_data)
                conn.commit()
                print(f"✅ Committed {len(customers_data)} customers batch")
                customers_data = []
            except Exception as e:
                print(f"❌ Customer batch error: {e}")
                conn.rollback()
                time.sleep(1)
    
    # Коммит оставшихся записей
    if customers_data:
        try:
            cursor.executemany("""
                INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO NOTHING
            """, customers_data)
            conn.commit()
            print(f"✅ Committed final {len(customers_data)} customers")
        except Exception as e:
            print(f"❌ Final customers error: {e}")
            conn.rollback()
    
    conn.close()

def generate_products():
    """Генерация тестовых продуктов"""
    conn = create_connection()
    cursor = conn.cursor()
    
    categories = ['electronics', 'home', 'books', 'sports', 'fashion', 'beauty', 'toys', 'garden', 'tools', 'health', 'automotive', 'baby']
    products_data = []
    
    for i in range(200):
        product_id = f"load_prod_{i:05d}"
        products_data.append((
            product_id,
            random.choice(categories),
            random.randint(10, 100),
            random.randint(50, 500),
            random.randint(1, 5),
            random.randint(100, 5000),
            random.randint(10, 50),
            random.randint(5, 30),
            random.randint(5, 30)
        ))
        
        # Коммит каждые 40 записей
        if len(products_data) % 40 == 0:
            try:
                cursor.executemany("""
                    INSERT INTO products (product_id, product_category_name, product_name_lenght, 
                                         product_description_lenght, product_photos_qty, product_weight_g,
                                         product_length_cm, product_height_cm, product_width_cm)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (product_id) DO NOTHING
                """, products_data)
                conn.commit()
                print(f"✅ Committed {len(products_data)} products batch")
                products_data = []
            except Exception as e:
                print(f"❌ Products batch error: {e}")
                conn.rollback()
                time.sleep(1)
    
    if products_data:
        try:
            cursor.executemany("""
                INSERT INTO products (product_id, product_category_name, product_name_lenght, 
                                     product_description_lenght, product_photos_qty, product_weight_g,
                                     product_length_cm, product_height_cm, product_width_cm)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
            """, products_data)
            conn.commit()
            print(f"✅ Committed final {len(products_data)} products")
        except Exception as e:
            print(f"❌ Final products error: {e}")
            conn.rollback()
    
    conn.close()

def generate_sellers():
    """Генерация тестовых продавцов"""
    conn = create_connection()
    cursor = conn.cursor()
    
    sellers_data = []
    for i in range(100):  # Увеличено с 20 до 100
        sellers_data.append((
            f"load_seller_{i:05d}",
            random.randint(1000, 99999),
            random.choice(['Sao Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Porto Alegre', 'Salvador']),
            random.choice(['SP', 'RJ', 'MG', 'RS', 'BA'])
        ))
        
        # Коммит каждые 25 записей
        if len(sellers_data) % 25 == 0:
            try:
                cursor.executemany("""
                    INSERT INTO sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (seller_id) DO NOTHING
                """, sellers_data)
                conn.commit()
                print(f"✅ Committed {len(sellers_data)} sellers batch")
                sellers_data = []
            except Exception as e:
                print(f"❌ Sellers batch error: {e}")
                conn.rollback()
                time.sleep(1)
    
    if sellers_data:
        try:
            cursor.executemany("""
                INSERT INTO sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (seller_id) DO NOTHING
            """, sellers_data)
            conn.commit()
            print(f"✅ Committed final {len(sellers_data)} sellers")
        except Exception as e:
            print(f"❌ Final sellers error: {e}")
            conn.rollback()
    
    conn.close()

def read_workload():
    """Нагрузка на чтение - увеличенное количество запросов"""
    conn = create_connection()
    cursor = conn.cursor()
    
    read_queries = [
        # Базовые агрегации
        "SELECT COUNT(*) FROM customers",
        "SELECT COUNT(*) FROM products",
        "SELECT COUNT(*) FROM orders",
        "SELECT COUNT(*) FROM sellers",
        "SELECT AVG(price) FROM order_items",
        "SELECT SUM(payment_value) FROM order_payments",
        
        # Группировки
        "SELECT customer_state, COUNT(*) FROM customers GROUP BY customer_state",
        "SELECT product_category_name, COUNT(*) FROM products GROUP BY product_category_name",
        "SELECT order_status, COUNT(*) FROM orders GROUP BY order_status",
        "SELECT seller_state, COUNT(*) FROM sellers GROUP BY seller_state",
        "SELECT payment_type, COUNT(*) FROM order_payments GROUP BY payment_type",
        
        # Сложные JOIN запросы
        """
        SELECT c.customer_state, COUNT(DISTINCT o.order_id) as order_count
        FROM customers c 
        LEFT JOIN orders o ON c.customer_id = o.customer_id 
        GROUP BY c.customer_state 
        ORDER BY order_count DESC
        """,
        """
        SELECT p.product_category_name, AVG(oi.price) as avg_price
        FROM products p 
        JOIN order_items oi ON p.product_id = oi.product_id 
        GROUP BY p.product_category_name 
        ORDER BY avg_price DESC
        """,
        """
        SELECT s.seller_state, COUNT(DISTINCT oi.order_id) as orders_count
        FROM sellers s 
        JOIN order_items oi ON s.seller_id = oi.seller_id 
        GROUP BY s.seller_state 
        ORDER BY orders_count DESC
        """,
        """
        SELECT EXTRACT(HOUR FROM order_purchase_timestamp) as hour, COUNT(*) as orders
        FROM orders 
        GROUP BY hour 
        ORDER BY hour
        """,
        """
        SELECT payment_type, AVG(payment_value) as avg_payment
        FROM order_payments 
        GROUP BY payment_type 
        ORDER BY avg_payment DESC
        """,
        
        # Подзапросы
        """
        SELECT customer_state, avg_orders
        FROM (
            SELECT c.customer_state, COUNT(o.order_id) as avg_orders
            FROM customers c 
            LEFT JOIN orders o ON c.customer_id = o.customer_id 
            GROUP BY c.customer_state
        ) sub
        WHERE avg_orders > 0
        ORDER BY avg_orders DESC
        """,
        
        # Аналитические функции
        """
        SELECT product_category_name, price_rank
        FROM (
            SELECT p.product_category_name, oi.price,
                   RANK() OVER (PARTITION BY p.product_category_name ORDER BY oi.price DESC) as price_rank
            FROM products p 
            JOIN order_items oi ON p.product_id = oi.product_id 
        ) ranked
        WHERE price_rank <= 3
        """,
        
        # Фильтрация по дате
        """
        SELECT DATE(order_purchase_timestamp) as order_date, COUNT(*) as daily_orders
        FROM orders 
        WHERE order_purchase_timestamp > NOW() - INTERVAL '30 days'
        GROUP BY order_date 
        ORDER BY order_date DESC
        """
    ]
    
    query_count = 0
    while True:
        try:
            query = random.choice(read_queries)
            start_time = time.time()
            cursor.execute(query)
            result = cursor.fetchall()
            execution_time = time.time() - start_time
            
            query_count += 1
            print(f"📊 [{query_count}] Read query executed in {execution_time:.3f}s: {query[:80]}...")
            
            # Коммит после каждых 5 запросов чтения (для освобождения ресурсов)
            if query_count % 5 == 0:
                conn.commit()
                print(f"🔄 Read workload commit #{query_count//5}")
            
            time.sleep(random.uniform(0.05, 0.2))  # Уменьшена задержка для большего количества запросов
            
        except Exception as e:
            print(f"❌ Read error: {e}")
            conn.rollback()
            print("🔄 Read workload rollback")
            time.sleep(0.5)
            try:
                conn.close()
            except:
                pass
            conn = create_connection()
            cursor = conn.cursor()

def write_workload():
    """Нагрузка на запись - увеличенное количество коммитов"""
    conn = create_connection()
    cursor = conn.cursor()
    
    write_count = 0
    batch_size = 3  # Коммит после каждых 3 операций записи
    
    while True:
        try:
            operations_in_batch = 0
            
            # Выполняем несколько операций записи перед коммитом
            for _ in range(batch_size):
                # Создание нового заказа
                order_id = f"load_order_{int(time.time())}_{random.randint(1000, 9999)}"
                customer_id = f"load_cust_{random.randint(0, 499):05d}"
                
                # Вставка заказа
                cursor.execute("""
                    INSERT INTO orders (order_id, customer_id, order_status, 
                                      order_purchase_timestamp, order_approved_at,
                                      order_estimated_delivery_date)
                    VALUES (%s, %s, %s, NOW(), NOW(), NOW() + INTERVAL '10 days')
                    ON CONFLICT (order_id) DO NOTHING
                """, (order_id, customer_id, random.choice(['processing', 'approved', 'shipped', 'created'])))
                operations_in_batch += 1
                
                # Вставка элементов заказа
                if random.random() > 0.2:  # 80% chance to add order items
                    for item_id in range(1, random.randint(1, 5)):
                        cursor.execute("""
                            INSERT INTO order_items (order_id, order_item_id, product_id, seller_id,
                                                  shipping_limit_date, price, freight_value)
                            VALUES (%s, %s, %s, %s, NOW() + INTERVAL '5 days', %s, %s)
                            ON CONFLICT (order_id, order_item_id) DO NOTHING
                        """, (
                            order_id, item_id, 
                            f"load_prod_{random.randint(0, 199):05d}",
                            f"load_seller_{random.randint(0, 99):05d}",
                            round(random.uniform(10, 500), 2),
                            round(random.uniform(5, 50), 2)
                        ))
                        operations_in_batch += 1
                
                # Вставка платежа
                if random.random() > 0.1:  # 90% chance to add payment
                    cursor.execute("""
                        INSERT INTO order_payments (order_id, payment_sequential, payment_type,
                                                  payment_installments, payment_value)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        order_id, random.randint(1, 3),
                        random.choice(['credit_card', 'boleto', 'voucher', 'debit_card']),
                        random.randint(1, 12),
                        round(random.uniform(20, 600), 2)
                    ))
                    operations_in_batch += 1
            
            # Коммит батча
            conn.commit()
            write_count += 1
            print(f"✍️ Write: Committed batch #{write_count} with {operations_in_batch} operations")
            
            # Случайный откат для тестирования (5% chance)
            if random.random() < 0.05:
                print("🔄 Simulating random write rollback...")
                conn.rollback()
                print("🔄 Write rollback completed")
                time.sleep(1)
            
            time.sleep(random.uniform(0.1, 0.3))  # Уменьшена задержка
            
        except Exception as e:
            print(f"❌ Write error: {e}")
            conn.rollback()
            print("🔄 Write workload rollback due to error")
            time.sleep(1)
            try:
                conn.close()
            except:
                pass
            conn = create_connection()
            cursor = conn.cursor()

def update_workload():
    """Нагрузка на обновление - исправлен синтаксис LIMIT"""
    conn = create_connection()
    cursor = conn.cursor()
    
    update_count = 0
    
    while True:
        try:
            # Исправленные запросы без LIMIT в неправильных местах
            update_operations = [
                """
                UPDATE orders 
                SET order_status = 'delivered',
                    order_delivered_customer_date = NOW()
                WHERE order_status = 'shipped' 
                AND order_purchase_timestamp < NOW() - INTERVAL '2 days'
                AND order_id IN (
                    SELECT order_id FROM orders 
                    WHERE order_status = 'shipped'
                    AND order_purchase_timestamp < NOW() - INTERVAL '2 days'
                    ORDER BY order_purchase_timestamp 
                    LIMIT 5
                )
                """,
                """
                UPDATE products 
                SET product_weight_g = product_weight_g + (random() * 10),
                    product_photos_qty = GREATEST(1, product_photos_qty - 1)
                WHERE product_id IN (
                    SELECT product_id FROM products 
                    WHERE product_id LIKE 'load_prod_%'
                    ORDER BY random() 
                    LIMIT 3
                )
                """,
                """
                UPDATE order_items 
                SET price = price * (0.9 + random() * 0.2)
                WHERE order_id IN (
                    SELECT order_id FROM orders 
                    WHERE order_purchase_timestamp > NOW() - INTERVAL '1 hour'
                    ORDER BY order_purchase_timestamp 
                    LIMIT 10
                )
                """,
                """
                UPDATE customers 
                SET customer_city = 
                    CASE 
                        WHEN customer_city = 'Sao Paulo' THEN 'São Paulo'
                        WHEN customer_city = 'Rio de Janeiro' THEN 'Rio'
                        ELSE customer_city
                    END
                WHERE customer_id IN (
                    SELECT customer_id FROM customers 
                    WHERE customer_id LIKE 'load_cust_%'
                    ORDER BY random() 
                    LIMIT 8
                )
                """,
                """
                UPDATE order_payments 
                SET payment_installments = payment_installments + 1,
                    payment_value = payment_value * 1.1
                WHERE payment_type = 'credit_card'
                AND order_id IN (
                    SELECT order_id FROM order_payments 
                    WHERE payment_type = 'credit_card'
                    ORDER BY random() 
                    LIMIT 6
                )
                """
            ]
            
            query = random.choice(update_operations)
            start_time = time.time()
            cursor.execute(query)
            affected_rows = cursor.rowcount
            
            # Коммит после каждого UPDATE
            conn.commit()
            update_count += 1
            
            execution_time = time.time() - start_time
            print(f"🔄 Update #{update_count}: {affected_rows} rows affected in {execution_time:.3f}s")
            
            # Случайный откат (8% chance)
            if random.random() < 0.08:
                print("🔄 Simulating random update rollback...")
                # Создаем фиктивный UPDATE который завершится ошибкой
                try:
                    cursor.execute("UPDATE non_existent_table SET col = 1")
                except:
                    pass
                conn.rollback()
                print("🔄 Update rollback completed")
            
            time.sleep(random.uniform(0.5, 2.0))
            
        except Exception as e:
            print(f"❌ Update error: {e}")
            conn.rollback()
            print("🔄 Update workload rollback due to error")
            time.sleep(2)
            try:
                conn.close()
            except:
                pass
            conn = create_connection()
            cursor = conn.cursor()

def maintenance_workload():
    """Операции обслуживания - исправлен синтаксис"""
    conn = create_connection()
    cursor = conn.cursor()
    
    maintenance_count = 0
    
    while True:
        try:
            maintenance_operations = [
                "CREATE TEMP TABLE IF NOT EXISTS temp_session_data AS SELECT * FROM orders WHERE order_purchase_timestamp > NOW() - INTERVAL '1 day'",
                "CREATE TEMP TABLE IF NOT EXISTS temp_products AS SELECT * FROM products WHERE product_category_name IN ('electronics', 'books')",
                "DROP TABLE IF EXISTS temp_old_data",
                "CREATE TEMP TABLE temp_old_data AS SELECT * FROM orders WHERE order_purchase_timestamp < NOW() - INTERVAL '30 days'",
            ]
            
            # Выполняем операции обслуживания
            for operation in random.sample(maintenance_operations, 2):
                cursor.execute(operation)
            
            # Анализ таблиц (30% chance)
            if random.random() > 0.7:
                tables = ['orders', 'customers', 'products', 'order_items', 'sellers']
                table = random.choice(tables)
                cursor.execute(f"ANALYZE {table}")
                print(f"🔧 Maintenance: ANALYZE {table} executed")
            
            # Очистка старых тестовых данных с правильным синтаксисом
            cursor.execute("""
                DELETE FROM orders 
                WHERE order_id LIKE 'load_order_%' 
                AND order_purchase_timestamp < NOW() - INTERVAL '1 hour'
                AND order_id IN (
                    SELECT order_id FROM orders 
                    WHERE order_id LIKE 'load_order_%'
                    AND order_purchase_timestamp < NOW() - INTERVAL '1 hour'
                    ORDER BY order_purchase_timestamp 
                    LIMIT 15
                )
            """)
            deleted_orders = cursor.rowcount
            
            conn.commit()
            maintenance_count += 1
            
            print(f"🔧 Maintenance #{maintenance_count}: Cleaned {deleted_orders} old orders")
            
            # Случайный откат (10% chance)
            if random.random() < 0.1:
                print("🔄 Simulating maintenance rollback...")
                try:
                    cursor.execute("ANALYZE non_existent_table")
                except:
                    pass
                conn.rollback()
                print("🔄 Maintenance rollback completed")
            
            time.sleep(random.uniform(3, 8))
            
        except Exception as e:
            print(f"❌ Maintenance error: {e}")
            conn.rollback()
            print("🔄 Maintenance workload rollback due to error")
            time.sleep(5)
            try:
                conn.close()
            except:
                pass
            conn = create_connection()
            cursor = conn.cursor()

def start_load_test():
    """Запуск всех типов нагрузки"""
    print("🚀 Starting enhanced Olist database load test...")
    print("📈 Features: More queries, frequent commits, rollback testing")
    
    # Инициализация тестовых данных
    generate_customers()
    generate_products()
    generate_sellers()
    
    # Запуск различных типов нагрузки в отдельных потоках
    with ThreadPoolExecutor(max_workers=12) as executor:  # Увеличено количество workers
        # Multiple read threads
        for i in range(4):
            executor.submit(read_workload)
        
        # Multiple write threads  
        for i in range(3):
            executor.submit(write_workload)
        
        # Update and maintenance
        executor.submit(update_workload)
        executor.submit(maintenance_workload)
        
        print("✅ All load generators started (12 threads). Press Ctrl+C to stop.")
        print("📊 Monitoring: Commits every few operations, random rollbacks for testing")
        
        # Бесконечный цикл
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("🛑 Stopping load test...")

if __name__ == "__main__":
    start_load_test()