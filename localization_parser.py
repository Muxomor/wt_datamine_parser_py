import csv
import requests
from typing import Dict, Any, List, Optional

from utils import Config, Logger


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
        self.localization_data: Dict[str, str] = {}
        
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
        """Парсит CSV данные локализации и извлекает русские названия"""
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
                    
                    unit_id = row[0].strip()
                    russian_name = row[6].strip()  # Колонка "Russian" (индекс 6)
                    
                    # Определяем ключ для хранения
                    storage_key = None
                    
                    # Обрабатываем записи с суффиксом _shop
                    if unit_id.endswith('_shop'):
                        storage_key = unit_id[:-5]  # Убираем '_shop'
                        self.logger.log(f"    _shop запись: {unit_id} -> ключ: {storage_key}", 'debug')
                    
                    # Обрабатываем групповые записи
                    elif unit_id.startswith('shop/group/'):
                        storage_key = unit_id[11:]  # Убираем 'shop/group/'
                        self.logger.log(f"    группа: {unit_id} -> ключ: {storage_key}", 'debug')
                    
                    # Обрабатываем записи с числовыми суффиксами (например, cn_t_62_0, he_112v_5_1)
                    elif '_' in unit_id and (unit_id.split('_')[-1].isdigit() or 
                                           any(unit_id.endswith('_' + str(i)) for i in range(10))):
                        # Сохраняем с полным ключом для дальнейшего поиска
                        storage_key = unit_id
                        self.logger.log(f"    числовой суффикс: {unit_id} -> ключ: {storage_key}", 'debug')
                    
                    # Если нашли подходящую запись
                    if storage_key:
                        if russian_name and russian_name != unit_id and russian_name.strip():
                            self.localization_data[storage_key] = russian_name
                            entries_count += 1
                            self.logger.log(f"    сохранено: {storage_key} -> {russian_name}", 'debug')
                        else:
                            # Если русское название пустое, используем исходный ID
                            self.localization_data[storage_key] = storage_key
                            self.logger.log(f"    fallback: {storage_key} -> {storage_key}", 'debug')
                            entries_count += 1
                    
                    processed_count += 1
                    
                except Exception as e:
                    self.logger.log(f"Ошибка обработки строки {line_num}: {e}", 'warning')
                    continue
            
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
        """Создает маппинг ID -> локализованное название"""
        localization_mapping = []
        found_count = 0
        not_found_count = 0
        
        self.logger.log("Создание маппинга локализации...")
        
        for unit_id in shop_ids:
            localized_name = self._find_localization_for_id(unit_id)
            
            if localized_name != unit_id:  # Если найдена локализация (не fallback)
                found_count += 1
                self.logger.log(f"  Найдено: {unit_id} -> {localized_name}", 'debug')
            else:
                not_found_count += 1
                self.logger.log(f"  Не найдено: {unit_id} -> {unit_id} (fallback)", 'debug')
            
            localization_mapping.append({
                'id': unit_id,
                'localized_name': localized_name
            })
        
        self.logger.log(f"Статистика локализации:")
        self.logger.log(f"  Найдено локализаций: {found_count}")
        self.logger.log(f"  Использован fallback: {not_found_count}")
        self.logger.log(f"  Всего обработано: {len(shop_ids)}")
        
        return localization_mapping
    
    def _find_localization_for_id(self, unit_id: str) -> str:
        """Ищет локализацию для конкретного ID с различными стратегиями поиска"""
        
        self.logger.log(f"    Поиск локализации для: {unit_id}", 'debug')
        
        # Стратегия 1: Прямой поиск
        if unit_id in self.localization_data:
            result = self.localization_data[unit_id]
            self.logger.log(f"    Прямой поиск: {unit_id} -> {result}", 'debug')
            return result
        
        # Стратегия 2: Поиск среди всех ключей с паттернами
        matching_keys = []
        
        # Собираем все возможные варианты ключей
        for key in self.localization_data.keys():
            # Точное совпадение (уже проверили выше)
            if key == unit_id:
                continue
                
            # Ключ начинается с unit_id + '_' (числовые суффиксы)
            if key.startswith(unit_id + '_'):
                matching_keys.append(key)
                self.logger.log(f"    Найден вариант с суффиксом: {key}", 'debug')
                
            # Для случаев, когда unit_id может быть частью более длинного ключа
            # Например, cn_t_62 может соответствовать cn_t_62_shop, cn_t_62_0, etc.
            if unit_id in key and (key.endswith('_shop') or 
                                 key.endswith('_0') or key.endswith('_1') or key.endswith('_2')):
                if key not in matching_keys:
                    matching_keys.append(key)
                    self.logger.log(f"    Найден вариант по паттерну: {key}", 'debug')
        
        if matching_keys:
            # Определяем приоритет поиска
            # 1. unit_id + '_shop' (основной вариант для техники)
            # 2. unit_id + '_0' (первый числовой вариант)
            # 3. unit_id + '_1', '_2' и т.д.
            # 4. Любой другой вариант
            
            priority_patterns = [
                unit_id + '_shop',
                unit_id + '_0',
                unit_id + '_1', 
                unit_id + '_2',
                unit_id + '_3',
                unit_id + '_4',
                unit_id + '_5'
            ]
            
            # Ищем по приоритету
            for priority_key in priority_patterns:
                if priority_key in matching_keys:
                    result = self.localization_data[priority_key]
                    self.logger.log(f"    Поиск по приоритету: {unit_id} -> {priority_key} -> {result}", 'debug')
                    return result
            
            # Если ничего по приоритету не найдено, берем первый найденный
            first_match = matching_keys[0]
            result = self.localization_data[first_match]
            self.logger.log(f"    Поиск по совпадению: {unit_id} -> {first_match} -> {result}", 'debug')
            return result
        
        # Стратегия 3: Специальная обработка для групп
        # Проверяем, есть ли в данных ключи, которые могут быть связаны с группой
        if unit_id.endswith('_group'):
            # Ищем соответствующую запись shop/group/
            group_key = 'shop/group/' + unit_id
            for key in self.localization_data.keys():
                if key == group_key or key.endswith('/' + unit_id):
                    result = self.localization_data[key]
                    self.logger.log(f"    Поиск группы: {unit_id} -> {key} -> {result}", 'debug')
                    return result
        
        # Стратегия 4: Fallback - возвращаем сам ID
        self.logger.log(f"    Fallback: {unit_id} -> {unit_id}", 'debug')
        return unit_id
    
    def save_to_csv(self, localization_mapping: List[Dict[str, str]], 
                    filename: str = 'localization.csv'):
        """Сохраняет маппинг локализации в CSV файл"""
        if not localization_mapping:
            self.logger.log("Нет данных локализации для сохранения", 'warning')
            return
            
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['id', 'localized_name']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for item in localization_mapping:
                    writer.writerow(item)
                    
            self.logger.log(f"Данные локализации ({len(localization_mapping)} записей) сохранены в {filename}")
            
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