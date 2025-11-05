#!/usr/bin/env python3
import psycopg2
import time
import threading
import logging
from concurrent.futures import ThreadPoolExecutor

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

class ConnectionKeeper:
    def __init__(self):
        self.connections = []
        self.lock = threading.Lock()
        self.running = True
    
    def create_connection(self, conn_id):
        """Создает и поддерживает одно постоянное соединение"""
        try:
            conn = psycopg2.connect(
                host="localhost",
                port="5432",
                database="postgres", 
                user="postgres",
                password="postgres",
                keepalives=1,
                keepalives_idle=60,
                keepalives_interval=10,
                keepalives_count=5
            )
            
            with self.lock:
                self.connections.append(conn)
            
            logger.info(f"✅ Соединение {conn_id} установлено. Активных: {len(self.connections)}")
            
            # Поддерживаем соединение активным
            while self.running:
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    time.sleep(5)  # Проверка каждые 5 секунд
                except Exception as e:
                    logger.error(f"❌ Ошибка в соединении {conn_id}: {e}")
                    break
            
            # Закрываем соединение при выходе
            conn.close()
            with self.lock:
                if conn in self.connections:
                    self.connections.remove(conn)
            logger.info(f"🔴 Соединение {conn_id} закрыто. Активных: {len(self.connections)}")
            
        except Exception as e:
            logger.error(f"❌ Не удалось установить соединение {conn_id}: {e}")
    
    def start_connections(self, num_connections):
        """Запускает указанное количество постоянных соединений"""
        logger.info(f"🚀 Запуск {num_connections} постоянных соединений...")
        
        with ThreadPoolExecutor(max_workers=num_connections) as executor:
            # Запускаем все соединения
            futures = []
            for i in range(num_connections):
                future = executor.submit(self.create_connection, i+1)
                futures.append(future)
                time.sleep(0.1)  # Небольшая задержка между созданиями
            
            # Ждем завершения (никогда не произойдет, пока running=True)
            try:
                while self.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("🛑 Получен сигнал остановки...")
                self.stop()
    
    def stop(self):
        """Останавливает все соединения"""
        self.running = False
        logger.info("🔴 Закрытие всех соединений...")
        
        with self.lock:
            for conn in self.connections:
                try:
                    conn.close()
                except:
                    pass
            self.connections.clear()
        
        logger.info("✅ Все соединения закрыты")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='PostgreSQL Connection Keeper')
    parser.add_argument('--connections', type=int, default=10, 
                       help='Количество постоянных соединений (по умолчанию: 10)')
    
    args = parser.parse_args()
    
    keeper = ConnectionKeeper()
    
    try:
        keeper.start_connections(args.connections)
    except KeyboardInterrupt:
        keeper.stop()
        logger.info("👋 Скрипт завершен")

if __name__ == "__main__":
    main()