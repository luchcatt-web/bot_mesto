"""
Генератор QR-кода для бота.
Запустите: python generate_qr.py
"""

import qrcode
from pathlib import Path

# Замените на username вашего бота (без @)
BOT_USERNAME = "your_bot_username"

def generate_qr():
    bot_link = f"https://t.me/{BOT_USERNAME}"
    
    # Создаём QR-код
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=10,
        border=4,
    )
    qr.add_data(bot_link)
    qr.make(fit=True)
    
    # Создаём изображение
    img = qr.make_image(fill_color="black", back_color="white")
    
    # Сохраняем
    output_path = Path(__file__).parent / "qr_code.png"
    img.save(output_path)
    
    print(f"✅ QR-код создан: {output_path}")
    print(f"📱 Ссылка: {bot_link}")
    print("\n💡 Распечатайте QR-код и разместите на ресепшене!")

if __name__ == "__main__":
    if BOT_USERNAME == "your_bot_username":
        print("⚠️  Сначала укажите username бота в переменной BOT_USERNAME")
        BOT_USERNAME = input("Введите username бота (без @): ").strip()
    
    try:
        generate_qr()
    except ImportError:
        print("📦 Установите библиотеку: pip install qrcode[pil]")

