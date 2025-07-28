"""
Главный модуль для запуска парсера shop.blkx War Thunder
"""

import sys
import os
from typing import Optional

from shop_parser import ShopParser
from localization_parser import LocalizationParser
from wpcost_parser import WpcostParser
from misc_and_images_parser import MiscAndImagesParser
from node_merger import ModernNodesMerger


def main(config_path: Optional[str] = None):
    """
    Основная функция запуска приложения (полный парсинг + объединение данных)
    
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
            print("localization_url=https://example.com/localization.csv")
            print("wpcost_url=https://example.com/wpcost.blkx")
            print("rank_url=https://example.com/rank.blkx")
            sys.exit(1)
        
        # 1. Создаем экземпляр основного парсера
        print("🚀 Запуск парсера shop.blkx...")
        parser = ShopParser(config_path)
        
        # Запускаем основной парсинг
        parser.run()
        
        print("✅ Основной парсинг успешно завершен!")
        print("📄 Результаты сохранены в файлы:")
        print("   - shop.csv (основные данные)")
        print("   - shop_images_fields.csv (поля image для fallback)")
        
        # 2. Запускаем парсинг локализации
        print("\n🌐 Запуск парсера локализации...")
        localization_parser = LocalizationParser(config_path)
        
        try:
            localization_parser.run()
            print("✅ Парсинг локализации успешно завершен!")
            print("📄 Результаты сохранены в файл localization.csv")
        except Exception as e:
            print(f"⚠️ Ошибка при парсинге локализации: {e}")
            print("💡 Основной парсинг завершен успешно, продолжаем с wpcost...")
        
        # 3. Запускаем парсинг wpcost
        print("\n💰 Запуск парсера wpcost...")
        wpcost_parser = WpcostParser(config_path)
        
        try:
            wpcost_parser.run()
            print("✅ Парсинг wpcost успешно завершен!")
            print("📄 Результаты сохранены в файл wpcost.csv")
        except Exception as e:
            print(f"⚠️ Ошибка при парсинге wpcost: {e}")
            print("💡 Основные этапы завершены, продолжаем с misc данными...")
        
        # 4. Запускаем парсинг misc данных
        print("\n📊 Запуск парсера misc данных...")
        misc_parser = MiscAndImagesParser(config_path)
        
        try:
            misc_parser.run()
            print("✅ Парсинг misc данных успешно завершен!")
            print("📄 Результаты сохранены в файлы:")
            print("   - rank_requirements.csv (требования по рангам)")
            print("   - shop_images.csv (изображения техники)")
            print("   - country_flags.csv (флаги стран)")
        except Exception as e:
            print(f"⚠️ Ошибка при парсинге misc данных: {e}")
            print("💡 Основные этапы завершены, продолжаем с объединением данных...")
        
        # 5. НОВОЕ: Запускаем объединение данных
        print("\n🔗 Запуск объединения данных и создания зависимостей...")
        merger = ModernNodesMerger(config_path)
        
        try:
            merged_data, dependencies = merger.run_full_merge()
            
            print("✅ Объединение данных успешно завершено!")
            print("📄 Результаты сохранены в файлы:")
            print("   - vehicles_merged.csv (полные данные о технике)")
            print("   - dependencies.csv (граф зависимостей)")
            
            # Дополнительная статистика
            if merged_data and dependencies:
                vehicles_count = len([item for item in merged_data if item.get('type') == 'vehicle'])
                folders_count = len([item for item in merged_data if item.get('type') == 'folder'])
                
                print(f"\n📈 Итоговая статистика:")
                print(f"   - Всего узлов: {len(merged_data)}")
                print(f"   - Техника: {vehicles_count}")
                print(f"   - Папки: {folders_count}")
                print(f"   - Зависимости: {len(dependencies)}")
                
        except Exception as e:
            print(f"⚠️ Ошибка при объединении данных: {e}")
            print("💡 Основные файлы созданы, можно продолжить работу с ними")
        
        print(f"\n🎉 Все операции завершены!")
        print("📁 Созданные файлы:")
        print("   - shop.csv (основные данные)")
        print("   - shop_images_fields.csv (поля image для fallback)")
        print("   - localization.csv (локализованные названия)")
        print("   - wpcost.csv (экономические данные)")
        print("   - rank_requirements.csv (требования по рангам)")
        print("   - shop_images.csv (изображения техники)")
        print("   - country_flags.csv (флаги стран)")
        print("   - vehicles_merged.csv (объединенные данные техники)")
        print("   - dependencies.csv (граф зависимостей)")
        print("📝 Логи:")
        print("   - shop_parser_debug.log (лог основного парсера)")
        print("   - localization_parser_debug.log (лог парсера локализации)")
        print("   - wpcost_parser_debug.log (лог парсера wpcost)")
        print("   - misc_and_images_parser_debug.log (лог парсера misc данных)")
        print("   - nodes_merger_debug.log (лог объединения данных)")
        
    except KeyboardInterrupt:
        print("\n⚠️ Операция прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)


def main_shop_only(config_path: Optional[str] = None):
    """
    Запуск только основного парсера shop.blkx (без локализации и wpcost)
    
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
        print("📄 Результаты сохранены в файлы:")
        print("   - shop.csv")
        print("   - shop_images_fields.csv")
        
    except KeyboardInterrupt:
        print("\n⚠️ Операция прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)


def main_localization_only(config_path: Optional[str] = None):
    """
    Запуск только парсера локализации
    
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
            print("Создайте файл config.txt с localization_url")
            sys.exit(1)
        
        # Проверяем существование shop.csv
        if not os.path.exists('shop.csv'):
            print("Ошибка: Файл shop.csv не найден.")
            print("Сначала выполните основной парсинг или используйте команду без флагов")
            sys.exit(1)
        
        # Создаем экземпляр парсера локализации
        localization_parser = LocalizationParser(config_path)
        
        # Запускаем парсинг локализации
        localization_parser.run()
        
        print("\n✅ Парсинг локализации успешно завершен!")
        print("📄 Результаты сохранены в файл localization.csv")
        
    except KeyboardInterrupt:
        print("\n⚠️ Операция прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)


def main_wpcost_only(config_path: Optional[str] = None):
    """
    Запуск только парсера wpcost
    
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
            print("Создайте файл config.txt с wpcost_url")
            sys.exit(1)
        
        # Проверяем существование shop.csv
        if not os.path.exists('shop.csv'):
            print("Ошибка: Файл shop.csv не найден.")
            print("Сначала выполните основной парсинг или используйте команду без флагов")
            sys.exit(1)
        
        # Создаем экземпляр парсера wpcost
        wpcost_parser = WpcostParser(config_path)
        
        # Запускаем парсинг wpcost
        wpcost_parser.run()
        
        print("\n✅ Парсинг wpcost успешно завершен!")
        print("📄 Результаты сохранены в файл wpcost.csv")
        
    except KeyboardInterrupt:
        print("\n⚠️ Операция прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)


def main_misc_only(config_path: Optional[str] = None):
    """
    Запуск только парсера misc данных (требования по рангам + флаги стран)
    
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
            print("Создайте файл config.txt с rank_url")
            sys.exit(1)
        
        # Создаем экземпляр парсера misc данных
        misc_parser = MiscAndImagesParser(config_path)
        
        # Запускаем парсинг misc данных
        misc_parser.run()
        
        print("\n✅ Парсинг misc данных успешно завершен!")
        print("📄 Результаты сохранены в файлы:")
        print("   - rank_requirements.csv (требования по рангам)")
        print("   - country_flags.csv (флаги стран)")
        print("   - shop_images.csv (изображения техники)")
        
    except KeyboardInterrupt:
        print("\n⚠️ Операция прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)


def main_merge_only(config_path: Optional[str] = None):
    """
    Запуск только объединения данных (требует готовые CSV файлы)
    
    Args:
        config_path: Путь к конфигурационному файлу (по умолчанию 'config.txt')
    """
    try:
        # Определяем путь к конфигурационному файлу
        if config_path is None:
            config_path = 'config.txt'
        
        # Проверяем существование основного файла
        if not os.path.exists('shop.csv'):
            print("Ошибка: Файл shop.csv не найден.")
            print("Сначала выполните основной парсинг или используйте команду без флагов")
            sys.exit(1)
        
        # Создаем экземпляр мерджера
        merger = ModernNodesMerger(config_path)
        
        # Запускаем объединение данных
        merged_data, dependencies = merger.run_full_merge()
        
        print("\n✅ Объединение данных успешно завершено!")
        print("📄 Результаты сохранены в файлы:")
        print("   - vehicles_merged.csv (полные данные о технике)")
        print("   - dependencies.csv (граф зависимостей)")
        
        # Дополнительная статистика
        if merged_data and dependencies:
            vehicles_count = len([item for item in merged_data if item.get('type') == 'vehicle'])
            folders_count = len([item for item in merged_data if item.get('type') == 'folder'])
            
            print(f"\n📈 Статистика:")
            print(f"   - Всего узлов: {len(merged_data)}")
            print(f"   - Техника: {vehicles_count}")
            print(f"   - Папки: {folders_count}")
            print(f"   - Зависимости: {len(dependencies)}")
        
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
    print("  python main.py                         - полный парсинг (shop.blkx + локализация + wpcost + misc + объединение)")
    print("  python main.py --config path.txt       - полный парсинг с указанным конфигом")
    print("  python main.py --shop-only             - только парсинг shop.blkx")
    print("  python main.py --localization-only     - только парсинг локализации")
    print("  python main.py --wpcost-only           - только парсинг wpcost")
    print("  python main.py --misc-only             - только парсинг misc данных (ранги + флаги + изображения)")
    print("  ℹ️  python main.py --merge-only             - только объединение данных (требует готовые CSV)")
    print("  python main.py --help                  - показать эту справку")
    print()
    print("Требования:")
    print("  1. Файл конфигурации должен содержать:")
    print("     shop_url=https://example.com/shop.blkx")
    print("     localization_url=https://example.com/localization.csv")
    print("     wpcost_url=https://example.com/wpcost.blkx")
    print("     rank_url=https://example.com/rank.blkx")
    print()
    print("  2. Установленные зависимости:")
    print("     pip install requests")
    print()
    print("Результат:")
    print("  - shop.csv                          - основные данные в CSV формате")
    print("  - shop_images_fields.csv            - поля image для fallback")
    print("  - localization.csv                  - локализованные названия")
    print("  - wpcost.csv                        - экономические данные (серебро, опыт, БР)")
    print("  - rank_requirements.csv             - требования по рангам")
    print("  - country_flags.csv                 - флаги стран")
    print("  - shop_images.csv                   - изображения техники")
    print("  ℹ️  - vehicles_merged.csv               - полные объединенные данные о технике")
    print("  ℹ️  - dependencies.csv                  - граф зависимостей между техникой")
    print("  - shop_parser_debug.log             - подробный лог основного парсера")
    print("  - localization_parser_debug.log     - подробный лог парсера локализации")
    print("  - wpcost_parser_debug.log           - подробный лог парсера wpcost")
    print("  - misc_and_images_parser_debug.log  - подробный лог парсера misc данных")
    print("  ℹ️  - nodes_merger_debug.log            - подробный лог объединения данных")
    print()
    print("Примечания:")
    print("  - Для парсинга только локализации/wpcost/merge нужен готовый файл shop.csv")
    print("  - Если какой-то URL отсутствует, соответствующий этап будет пропущен")
    print("  - wpcost парсер вычисляет БР по формуле: (economicRankHistorical / 3) + 1")
    print("  - misc парсер проверяет доступность флагов стран и извлекает требования по рангам")
    print("  - misc парсер собирает изображения техники из shop.csv и проверяет их доступность")
    print("  ℹ️  - merge создает граф зависимостей на основе поля 'predecessor' из shop.csv")


if __name__ == "__main__":
    # Обработка аргументов командной строки
    if len(sys.argv) == 1:
        # Запуск без аргументов - полный парсинг
        main()
    elif len(sys.argv) == 2:
        arg = sys.argv[1]
        if arg in ['--help', '-h', 'help']:
            print_help()
        elif arg == '--shop-only':
            main_shop_only()
        elif arg == '--localization-only':
            main_localization_only()
        elif arg == '--wpcost-only':
            main_wpcost_only()
        elif arg == '--misc-only':
            main_misc_only()
        elif arg == '--merge-only':
            main_merge_only()
        else:
            print(f"Неизвестный аргумент: {arg}")
            print("Используйте --help для получения справки")
            sys.exit(1)
    elif len(sys.argv) == 3 and sys.argv[1] == '--config':
        # Запуск с указанным конфигом - полный парсинг
        config_file = sys.argv[2]
        main(config_file)
    elif len(sys.argv) == 4 and sys.argv[1] == '--config':
        # Запуск с указанным конфигом и флагом
        config_file = sys.argv[2]
        flag = sys.argv[3]
        if flag == '--shop-only':
            main_shop_only(config_file)
        elif flag == '--localization-only':
            main_localization_only(config_file)
        elif flag == '--wpcost-only':
            main_wpcost_only(config_file)
        elif flag == '--misc-only':
            main_misc_only(config_file)
        elif flag == '--merge-only':
            main_merge_only(config_file)
        else:
            print(f"Неизвестный флаг: {flag}")
            print("Используйте --help для получения справки")
            sys.exit(1)
    else:
        print("Неверное количество аргументов")
        print("Используйте --help для получения справки")
        sys.exit(1)