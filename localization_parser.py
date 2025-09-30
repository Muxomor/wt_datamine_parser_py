import csv
import requests
from typing import Dict, Any, List, Optional

from utils import Config, Logger, Constants


class LocalizationParser:
    """Класс для парсинга локализации юнитов War Thunder"""
    
    def __init__(self, config_path: str = 'config.txt'):
        """
        Инициализация парсера локализации
        
        Args:
            config_path: Путь к конфигурационному файлу
        """
        self.config = Config(config_path)
        self.logger = Logger('localization_parser', 'localization_parser_debug.log')
        self.localization_data: Dict[str, Dict[str, str]] = {}
        
    def fetch_localization_data(self) -> str:
        """Загружает данные локализации из удаленного источника"""
        localization_url = self.config.get('localization_url')
        if not localization_url:
            raise ValueError("localization_url не найден в конфигурации")
            
        try:
            self.logger.log(f"Загрузка данных локализации из: {localization_url}")
            response = requests.get(localization_url, timeout=30)
            response.raise_for_status()
            
            localization_content = response.text
            self.logger.log(f"Данные локализации успешно загружены")
            return localization_content
            
        except requests.RequestException as e:
            raise RuntimeError(f"Ошибка загрузки данных локализации: {e}")
    
    def parse_localization_csv(self, csv_content: str):
        """Парсит CSV данные локализации и извлекает русские и английские названия"""
        self.logger.log("Парсинг данных локализации...")
        
        try:
            # Разбираем CSV содержимое
            lines = csv_content.strip().split('\n')
            
            # Пропускаем заголовок
            if not lines:
                self.logger.log("Файл локализации пуст", 'warning')
                return
            
            header_line = lines[0] if lines else ""
            self.logger.log(f"Заголовок: {header_line[:100]}...", 'debug')
            
            # Словарь для временного хранения записей по приоритету
            temp_storage = {}
            
            # Обрабатываем каждую строку
            processed_count = 0
            entries_count = 0
            
            for line_num, line in enumerate(lines[1:], 2):  # Пропускаем заголовок
                if not line.strip():
                    continue
                    
                try:
                    # Парсим CSV строку с учетом кавычек и точек с запятой
                    reader = csv.reader([line], delimiter=';', quotechar='"')
                    row = next(reader)
                    
                    if len(row) < 7:  # Проверяем, что есть минимум 7 колонок (до русского языка)
                        self.logger.log(f"Строка {line_num}: недостаточно колонок ({len(row)})", 'debug')
                        continue
                    
                    unit_id = row[0].strip().lower()
                    english_name = row[1].strip() if len(row) > 1 else ""  # Колонка "English" (индекс 1)
                    russian_name = row[6].strip() if len(row) > 6 else ""  # Колонка "Russian" (индекс 6)
                    
                    # Пропускаем записи без названий или с некорректными данными
                    if not russian_name and not english_name:
                        continue
                    
                    # Если русского названия нет, но есть английское - используем английское как fallback
                    if not russian_name and english_name:
                        russian_name = english_name
                        
                    # Если английского названия нет, но есть русское - используем русское как fallback
                    if not english_name and russian_name:
                        english_name = russian_name
                    
                    storage_key = None
                    priority = 999 
                    
                    # Обрабатываем записи с суффиксом _shop (высший приоритет)
                    if unit_id.endswith('_shop'):
                        storage_key = unit_id[:-5]  # Убираем '_shop'
                        priority = 1
                        self.logger.log(f"    _shop запись: {unit_id} -> ключ: {storage_key} (приоритет: {priority})", 'debug')
                    
                    # Обрабатываем групповые записи (высший приоритет для групп)
                    elif unit_id.startswith('shop/group/'):
                        storage_key = unit_id[11:]  # Убираем 'shop/group/'
                        priority = 1
                        self.logger.log(f"    группа: {unit_id} -> ключ: {storage_key} (приоритет: {priority})", 'debug')
                    
                    # Обрабатываем записи с числовыми суффиксами (низкий приоритет)
                    elif '_' in unit_id and unit_id.split('_')[-1].isdigit():
                        storage_key = unit_id
                        priority = 10
                        self.logger.log(f"    числовой суффикс: {unit_id} -> ключ: {storage_key} (приоритет: {priority})", 'debug')
                    
                    # Если нашли подходящую запись
                    if storage_key and (russian_name != unit_id or english_name != unit_id):
                        # Проверяем, нужно ли обновить запись (сравниваем приоритеты)
                        if storage_key not in temp_storage or temp_storage[storage_key]['priority'] > priority:
                            temp_storage[storage_key] = {
                                'russian_name': russian_name,
                                'english_name': english_name,
                                'priority': priority,
                                'source': unit_id
                            }
                            self.logger.log(f"    сохранено/обновлено: {storage_key} -> RU: {russian_name}, EN: {english_name} (источник: {unit_id})", 'debug')
                        else:
                            self.logger.log(f"    пропущено (низкий приоритет): {storage_key} -> RU: {russian_name}, EN: {english_name} (источник: {unit_id})", 'debug')
                    
                    processed_count += 1
                    
                except Exception as e:
                    self.logger.log(f"Ошибка обработки строки {line_num}: {e}", 'warning')
                    continue
            
            # Переносим данные из временного хранилища в основной словарь
            for key, data in temp_storage.items():
                self.localization_data[key] = {
                    'russian_name': data['russian_name'],
                    'english_name': data['english_name']
                }
                entries_count += 1
            
            self.logger.log(f"Обработано строк: {processed_count}")
            self.logger.log(f"Найдено записей для локализации: {entries_count}")
            self.logger.log(f"Всего ключей в словаре: {len(self.localization_data)}")
            
        except Exception as e:
            self.logger.log(f"Ошибка парсинга CSV локализации: {e}", 'error')
            raise
    
    def load_shop_ids(self, shop_csv_path: str = 'shop.csv') -> List[str]:
        """Загружает список ID из файла shop.csv"""
        shop_ids = []
        
        try:
            with open(shop_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    unit_id = row.get('id', '').strip()
                    if unit_id:
                        shop_ids.append(unit_id)
                        
            self.logger.log(f"Загружено {len(shop_ids)} ID из {shop_csv_path}")
            return shop_ids
            
        except FileNotFoundError:
            raise RuntimeError(f"Файл {shop_csv_path} не найден. Сначала выполните основной парсинг shop.blkx")
        except Exception as e:
            raise RuntimeError(f"Ошибка чтения файла {shop_csv_path}: {e}")
    
    def create_localization_mapping(self, shop_ids: List[str]) -> List[Dict[str, str]]:
        """Создает маппинг ID -> локализованные названия (русское и английское)"""
        localization_mapping = []
        found_count = 0
        not_found_count = 0
        
        self.logger.log("Создание маппинга локализации...")
        
        for unit_id in shop_ids:
            localized_russian, localized_english = self._find_localization_for_id(unit_id)
            
            if localized_russian != unit_id or localized_english != unit_id:  # Если найдена хотя бы одна локализация
                found_count += 1
                self.logger.log(f"  Найдено: {unit_id} -> RU: {localized_russian}, EN: {localized_english}", 'debug')
            else:
                not_found_count += 1
                self.logger.log(f"  Не найдено: {unit_id} -> RU: {unit_id} (fallback), EN: {unit_id} (fallback)", 'debug')
            
            localization_mapping.append({
                'id': unit_id,
                'localized_name': localized_russian,
                'localized_name_eng': localized_english
            })
        
        self.logger.log(f"Статистика локализации:")
        self.logger.log(f"  Найдено локализаций: {found_count}")
        self.logger.log(f"  Использован fallback: {not_found_count}")
        self.logger.log(f"  Всего обработано: {len(shop_ids)}")
        
        return localization_mapping
    
    def _find_localization_for_id(self, unit_id: str) -> tuple[str, str]:
        """Ищет локализацию для конкретного ID с различными стратегиями поиска"""
        
        self.logger.log(f"    Поиск локализации для: {unit_id}", 'debug')
        
        # Стратегия 1: Прямой поиск точного совпадения
        if unit_id in self.localization_data:
            russian_name = self.localization_data[unit_id]['russian_name']
            english_name = self.localization_data[unit_id]['english_name']
            self.logger.log(f"    Прямой поиск: {unit_id} -> RU: {russian_name}, EN: {english_name}", 'debug')
            return russian_name, english_name
        
        # Стратегия 2: Точные совпадения с приоритетными суффиксами
        priority_patterns = [
            unit_id + '_shop',
            unit_id + '_1'
        ]
        
        for priority_key in priority_patterns:
            if priority_key in self.localization_data:
                russian_name = self.localization_data[priority_key]['russian_name']
                english_name = self.localization_data[priority_key]['english_name']
                self.logger.log(f"    Точное совпадение по приоритету: {unit_id} -> {priority_key} -> RU: {russian_name}, EN: {english_name}", 'debug')
                return russian_name, english_name
        
        # Стратегия 3: Поиск точных совпадений с любыми числовыми суффиксами
        # Ищем ключи, которые точно начинаются с unit_id + '_' + цифра
        exact_matches = []
        for key in self.localization_data.keys():
            # Проверяем, что ключ начинается с unit_id + '_'
            if key.startswith(unit_id + '_'):
                # Извлекаем суффикс после unit_id + '_'
                suffix = key[len(unit_id) + 1:]
                # Проверяем, что суффикс состоит только из цифр или известных паттернов
                if suffix.isdigit() or suffix in ['shop', '0', '1', '2', '3', '4', '5']:
                    exact_matches.append(key)
                    self.logger.log(f"    Найдено точное совпадение: {key}", 'debug')
        
        if exact_matches:
            # Сортируем по приоритету: сначала _shop, потом числовые по возрастанию
            def sort_priority(key):
                suffix = key[len(unit_id) + 1:]
                if suffix == 'shop':
                    return 0
                elif suffix.isdigit():
                    return int(suffix) + 1
                else:
                    return 999
            
            exact_matches.sort(key=sort_priority)
            best_match = exact_matches[0]
            russian_name = self.localization_data[best_match]['russian_name']
            english_name = self.localization_data[best_match]['english_name']
            self.logger.log(f"    Лучшее точное совпадение: {unit_id} -> {best_match} -> RU: {russian_name}, EN: {english_name}", 'debug')
            return russian_name, english_name
        
        # Стратегия 4: Специальная обработка для групп
        if unit_id.endswith('_group'):
            # Ищем соответствующую запись shop/group/
            for key in self.localization_data.keys():
                if key == unit_id or key.endswith('/' + unit_id):
                    russian_name = self.localization_data[key]['russian_name']
                    english_name = self.localization_data[key]['english_name']
                    self.logger.log(f"    Поиск группы: {unit_id} -> {key} -> RU: {russian_name}, EN: {english_name}", 'debug')
                    return russian_name, english_name
        
        # Стратегия 5: Fallback - возвращаем сам ID для обоих языков
        self.logger.log(f"    Fallback: {unit_id} -> RU: {unit_id}, EN: {unit_id}", 'debug')
        return unit_id, unit_id
    
    def save_to_csv(self, localization_mapping: List[Dict[str, str]], 
                    filename: str = 'localization.csv'):
        """Сохраняет маппинг локализации в CSV файл"""
        if not localization_mapping:
            self.logger.log("Нет данных локализации для сохранения", 'warning')
            return
            
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                fieldnames = Constants.LOCALIZATION_CSV_FIELDNAMES
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for item in localization_mapping:
                    writer.writerow(item)
                    
            self.logger.log(f"Данные локализации ({len(localization_mapping)} записей) сохранены в {filename}")
            
            # Дополнительная статистика
            russian_found = sum(1 for item in localization_mapping if item['localized_name'] != item['id'])
            english_found = sum(1 for item in localization_mapping if item['localized_name_eng'] != item['id'])
            
            self.logger.log(f"Статистика сохранения:")
            self.logger.log(f"  Русских локализаций: {russian_found}")
            self.logger.log(f"  Английских локализаций: {english_found}")
            self.logger.log(f"  Всего записей: {len(localization_mapping)}")
            
        except Exception as e:
            self.logger.log(f"Ошибка при сохранении локализации в CSV: {e}", 'error')
            raise
    
    def run(self, shop_csv_path: str = 'shop.csv', output_file: str = 'localization.csv'):
        """Основной метод запуска парсера локализации"""
        try:
            self.logger.log("Запуск парсера локализации...")
            
            # Загружаем данные локализации
            localization_content = self.fetch_localization_data()
            
            # Парсим данные локализации
            self.parse_localization_csv(localization_content)
            
            # Загружаем ID из shop.csv
            shop_ids = self.load_shop_ids(shop_csv_path)
            
            # Создаем маппинг локализации
            localization_mapping = self.create_localization_mapping(shop_ids)
            
            # Сохраняем результат
            self.save_to_csv(localization_mapping, output_file)
            
            self.logger.log(f"Парсинг локализации завершен успешно! Создан файл {output_file}")
            return True
            
        except Exception as e:
            self.logger.log(f"Ошибка при выполнении парсинга локализации: {e}", 'error')
            raise