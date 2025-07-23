#!/usr/bin/env python3
"""
Главный модуль для запуска парсера shop.blkx War Thunder
"""

import sys
import os
from typing import Optional

from shop_parser import ShopParser


def main(config_path: Optional[str] = None):
    """
    Основная функция запуска приложения
    
    Args:
        config_path: Путь к конфигурационному файлу (по умолчанию 'config.txt')
    """
    try:
        # Определяем путь к конфигурационному файлу
        if config_path is None:
            config_path = 'config.txt'
        
        # Проверяем существование конфигурационного файла
        if not os.path.exists(config_path):
            print(f"Ошибка: Конфигурационный файл '{config_path}' не найден.")
            print("Создайте файл config.txt со следующим содержимым:")
            print("shop_url=https://example.com/shop.blkx")
            sys.exit(1)
        
        # Создаем экземпляр парсера
        parser = ShopParser(config_path)
        
        # Запускаем парсинг
        parser.run()
        
        print("\n✅ Парсинг успешно завершен!")
        print("Результаты сохранены в файл shop.csv")
        
    except KeyboardInterrupt:
        print("\n⚠️ Операция прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)


def print_help():
    """Выводит справку по использованию"""
    print("Парсер shop.blkx для War Thunder")
    print("================================")
    print()
    print("Использование:")
    print("  python main.py                    - запуск с конфигом по умолчанию (config.txt)")
    print("  python main.py --config path.txt  - запуск с указанным конфигом")
    print("  python main.py --help             - показать эту справку")
    print()
    print("Требования:")
    print("  1. Файл конфигурации должен содержать:")
    print("     shop_url=https://example.com/shop.blkx")
    print()
    print("  2. Установленные зависимости:")
    print("     pip install requests")
    print()
    print("Результат:")
    print("  - shop.csv                - основные данные в CSV формате")
    print("  - shop_parser_debug.log   - подробный лог работы парсера")


if __name__ == "__main__":
    # Обработка аргументов командной строки
    if len(sys.argv) == 1:
        # Запуск без аргументов
        main()
    elif len(sys.argv) == 2:
        if sys.argv[1] in ['--help', '-h', 'help']:
            print_help()
        else:
            print(f"Неизвестный аргумент: {sys.argv[1]}")
            print("Используйте --help для получения справки")
            sys.exit(1)
    elif len(sys.argv) == 3 and sys.argv[1] == '--config':
        # Запуск с указанным конфигом
        config_file = sys.argv[2]
        main(config_file)
    else:
        print("Неверное количество аргументов")
        print("Используйте --help для получения справки")
        sys.exit(1)