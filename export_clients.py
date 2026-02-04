"""
Экспорт клиентов в CSV для импорта в YClients.
Запустите: python export_clients.py
"""

import csv
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "clients.db"
OUTPUT_PATH = Path(__file__).parent / f"clients_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


def export_to_csv():
    if not DB_PATH.exists():
        print("❌ База данных не найдена. Сначала запустите бота.")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT phone_number, first_name, last_name, telegram_id, created_at 
        FROM clients 
        ORDER BY created_at DESC
    """)
    
    clients = cursor.fetchall()
    conn.close()
    
    if not clients:
        print("📭 Пока нет сохранённых клиентов.")
        return
    
    # Записываем в CSV
    with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Заголовки совместимые с YClients
        writer.writerow(['Телефон', 'Имя', 'Фамилия', 'Telegram ID', 'Дата регистрации'])
        
        for client in clients:
            writer.writerow(client)
    
    print(f"✅ Экспортировано {len(clients)} клиентов")
    print(f"📁 Файл: {OUTPUT_PATH}")
    print("\n💡 Этот файл можно импортировать в YClients через раздел 'Клиенты' → 'Импорт'")


def show_stats():
    """Показать статистику"""
    if not DB_PATH.exists():
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM clients")
    total = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT COUNT(*) FROM clients 
        WHERE date(created_at) = date('now')
    """)
    today = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"\n📊 Статистика:")
    print(f"   Всего клиентов: {total}")
    print(f"   Новых сегодня: {today}")


if __name__ == "__main__":
    export_to_csv()
    show_stats()

