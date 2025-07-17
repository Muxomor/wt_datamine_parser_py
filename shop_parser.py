import json
import csv
import requests
from typing import Dict, List, Any, Optional
import sys
import os
import logging

class ShopParser:
    def __init__(self, config_path: str = 'config.txt'):
        """
        Инициализация парсера shop.blkx
        
        Args:
            config_path: Путь к конфигурационному файлу
        """
        self.config = self.read_config(config_path)
        self.vehicles_data = []
        
        # Настройка логирования
        self.setup_logging()
        
        # Маппинг типов техники из JSON в название для БД
        self.vehicle_type_mapping = {
            'army': 'Наземная техника',
            'aviation': 'Авиация', 
            'helicopters': 'Вертолёты',
            'ships': 'Большой флот',
            'boats': 'Малый флот'
        }
        
        # Маппинг стран
        self.country_mapping = {
            'country_usa': 'usa',
            'country_germany': 'germany',
            'country_ussr': 'ussr',
            'country_britain': 'britain',
            'country_japan': 'japan',
            'country_china': 'china',
            'country_italy': 'italy',
            'country_france': 'france',
            'country_sweden': 'sweden',
            'country_israel': 'israel'
        }

    def setup_logging(self):
        """Настройка логирования в файл и консоль"""
        # Создаем логгер
        self.logger = logging.getLogger('shop_parser')
        self.logger.setLevel(logging.DEBUG)
        
        # Очищаем существующие обработчики
        self.logger.handlers.clear()
        
        # Обработчик для файла
        file_handler = logging.FileHandler('shop_parser_debug.log', mode='w', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Обработчик для консоли
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Форматтер
        formatter = logging.Formatter('%(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Добавляем обработчики
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def log(self, message: str, level: str = 'info'):
        """Логирование сообщений"""
        if level == 'debug':
            self.logger.debug(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)
        else:
            self.logger.info(message)

    def read_config(self, config_path: str) -> Dict[str, str]:
        """Читает конфигурационный файл"""
        config = {}
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if '=' not in line:
                            self.log(f"Предупреждение: Пропускаем некорректную строку #{line_num}: {line}", 'warning')
                            continue
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip()
        except FileNotFoundError:
            raise RuntimeError(f"Конфигурационный файл '{config_path}' не найден.")
        except Exception as e:
            raise RuntimeError(f"Ошибка чтения конфига '{config_path}': {str(e)}")
        
        return config

    def fetch_shop_data(self) -> Dict[str, Any]:
        """Загружает данные shop.blkx из источника"""
        shop_url = self.config.get('shop_url')
        if not shop_url:
            raise ValueError("shop_url не найден в конфигурации")
            
        try:
            self.log(f"Загрузка данных из: {shop_url}")
            response = requests.get(shop_url, timeout=30)
            response.raise_for_status()
            
            shop_data = response.json()
            self.log(f"Данные успешно загружены")
            return shop_data
            
        except requests.RequestException as e:
            raise RuntimeError(f"Ошибка загрузки данных: {e}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Ошибка декодирования JSON: {e}")

    def is_group(self, item_name: str, item_data: Dict[str, Any]) -> bool:
        """Определяет является ли элемент группой"""
        # Проверяем окончание на _group (основной признак)
        if item_name.endswith('_group'):
            return True
            
        # Проверяем наличие image (дополнительный признак)
        if 'image' in item_data:
            return True
            
        # Проверяем наличие вложенных элементов (не служебные поля)
        service_fields = {
            'rank', 'reqAir', 'image', 'slaveUnit', 'reqUnlock', 'gift', 
            'marketplaceItemdefId', 'hideFeature', 'event', 'showOnlyWhenBought',
            'beginPurchaseDate', 'endPurchaseDate', 'isClanVehicle', 'reqFeature',
            'showByPlatform', 'costGold', 'freeRepairs', 'rankPosXY', 'fakeReqUnitType',
            'fakeReqUnitImage', 'fakeReqUnitRank', 'fakeReqUnitPosXY'
        }
        nested_items = [key for key in item_data.keys() if key not in service_fields]
        
        return len(nested_items) > 0

    def is_slave_unit(self, item_data: Dict[str, Any]) -> bool:
        """Определяет является ли элемент slave-юнитом"""
        return 'slaveUnit' in item_data

    def get_group_items(self, group_data: Dict[str, Any]) -> List[tuple]:
        """Извлекает элементы из группы с их порядковыми номерами"""
        service_fields = {
            'rank', 'reqAir', 'image', 'slaveUnit', 'reqUnlock', 'gift', 
            'marketplaceItemdefId', 'hideFeature', 'event', 'showOnlyWhenBought',
            'beginPurchaseDate', 'endPurchaseDate', 'isClanVehicle', 'reqFeature',
            'showByPlatform', 'costGold', 'freeRepairs', 'rankPosXY', 'fakeReqUnitType',
            'fakeReqUnitImage', 'fakeReqUnitRank', 'fakeReqUnitPosXY'
        }
        items = []
        
        for order, (item_name, item_data) in enumerate(group_data.items()):
            if item_name not in service_fields:
                # Добавляем отладочную информацию
                if not isinstance(item_data, dict):
                    self.log(f"        ОТЛАДКА: Элемент группы {item_name} имеет тип {type(item_data)}: {item_data}", 'debug')
                items.append((order, item_name, item_data))
                
        return items

    def process_range_column(self, range_data: Dict[str, Any], country: str, 
                           vehicle_type: str, column_index: int) -> List[Dict[str, Any]]:
        """Обрабатывает один столбец (range) техники"""
        results = []
        next_should_depend_on_group = None
        
        # Обрабатываем элементы в том порядке, в котором они идут в JSON
        for item_name, item_info in range_data.items():
            if not isinstance(item_info, dict):
                self.log(f"      ПРЕДУПРЕЖДЕНИЕ: Элемент {item_name} не является словарем: {type(item_info)}", 'warning')
                continue
                
            if self.is_group(item_name, item_info):
                # Обрабатываем группу
                rank = item_info.get('rank', 1)
                
                # Получаем элементы группы для определения правильного ранга
                group_items = self.get_group_items(item_info)
                if group_items:
                    # Ранг группы = ранг её первого элемента
                    first_item = group_items[0]
                    first_item_info = first_item[2]
                    
                    # Проверяем, что first_item_info является словарем
                    if isinstance(first_item_info, dict):
                        actual_rank = first_item_info.get('rank', rank)
                    else:
                        self.log(f"        ПРЕДУПРЕЖДЕНИЕ: Первый элемент группы {item_name} не является словарем: {type(first_item_info)}", 'warning')
                        actual_rank = rank
                else:
                    actual_rank = rank
                
                # Добавляем саму группу (если не slave-unit)
                group_item = None
                if not self.is_slave_unit(item_info):
                    # Получаем координаты из rankPosXY если есть
                    pos_xy = item_info.get('rankPosXY', [column_index, actual_rank - 1])
                    
                    # Определяем предшественника для группы
                    predecessor = ''
                    # Если поле reqAir отсутствует или не равно "", то есть зависимость
                    has_dependency = 'reqAir' not in item_info or item_info.get('reqAir') != ''
                    self.log(f"        ОТЛАДКА: Группа {item_name}, reqAir в JSON: {'есть' if 'reqAir' in item_info else 'нет'}, значение: '{item_info.get('reqAir', 'отсутствует')}', has_dependency: {has_dependency}", 'debug')
                    
                    if has_dependency:
                        # Проверяем, должна ли группа зависеть от предыдущей группы
                        if next_should_depend_on_group:
                            predecessor = next_should_depend_on_group
                            next_should_depend_on_group = None
                            self.log(f"        ОТЛАДКА: Группа {item_name} зависит от предыдущей группы {predecessor}", 'debug')
                        elif results:
                            predecessor = results[-1]['id']
                            self.log(f"        ОТЛАДКА: Группа {item_name} зависит от {predecessor}", 'debug')
                    else:
                        self.log(f"        ОТЛАДКА: Группа {item_name} имеет reqAir='', предшественника нет", 'debug')
                    
                    group_item = {
                        'id': item_name,
                        'rank': actual_rank,
                        'country': country,
                        'vehicle_type': vehicle_type,
                        'type': 'folder',
                        'reqAir': item_info.get('reqAir', None),  # Сохраняем оригинальное значение
                        'is_group': True,
                        'order_in_folder': None,
                        'predecessor': predecessor,
                        'column_index': pos_xy[0] if 'rankPosXY' in item_info else column_index,
                        'row_index': pos_xy[1] if 'rankPosXY' in item_info else actual_rank - 1
                    }
                    
                    results.append(group_item)
                    self.log(f"        ОТЛАДКА: Добавлена группа {item_name} с предшественником '{predecessor}'", 'debug')
                
                # Обрабатываем элементы внутри группы
                for order, nested_name, nested_info in group_items:
                    if not isinstance(nested_info, dict):
                        self.log(f"        ПРЕДУПРЕЖДЕНИЕ: Элемент группы {nested_name} не является словарем: {type(nested_info)} = {nested_info}", 'warning')
                        continue
                        
                    nested_rank = nested_info.get('rank', actual_rank)
                    
                    # Для slave-unit берем только основной элемент
                    if self.is_slave_unit(item_info):
                        parent_id = item_info.get('slaveUnit', '')
                    else:
                        parent_id = item_name
                    
                    # Получаем координаты из rankPosXY если есть
                    pos_xy = nested_info.get('rankPosXY', item_info.get('rankPosXY', [column_index, nested_rank - 1]))
                    
                    # Определяем предшественника для элемента группы
                    predecessor = ''
                    # Если поле reqAir отсутствует или не равно "", то есть зависимость
                    has_dependency = 'reqAir' not in nested_info or nested_info.get('reqAir') != ''
                    self.log(f"          ОТЛАДКА: Элемент {nested_name}, reqAir в JSON: {'есть' if 'reqAir' in nested_info else 'нет'}, значение: '{nested_info.get('reqAir', 'отсутствует')}', order={order}, parent_id={parent_id}, has_dependency: {has_dependency}", 'debug')
                    
                    if has_dependency:
                        if order == 0:
                            # Первый элемент группы зависит от самой группы
                            predecessor = parent_id
                            self.log(f"          ОТЛАДКА: Первый элемент группы {nested_name} зависит от группы {parent_id}", 'debug')
                        else:
                            # Остальные элементы зависят от предыдущего элемента группы
                            prev_order = order - 1
                            for prev_order_item, prev_name, prev_info in group_items:
                                if prev_order_item == prev_order:
                                    predecessor = prev_name
                                    self.log(f"          ОТЛАДКА: Элемент группы {nested_name} зависит от {prev_name}", 'debug')
                                    break
                    else:
                        self.log(f"          ОТЛАДКА: Элемент группы {nested_name} имеет reqAir='', предшественника нет", 'debug')
                        
                    nested_item = {
                        'id': nested_name,
                        'rank': nested_rank,
                        'country': country,
                        'vehicle_type': vehicle_type,
                        'type': 'vehicle',
                        'reqAir': nested_info.get('reqAir', None),  # Сохраняем оригинальное значение
                        'is_group': False,
                        'parent_id': parent_id,
                        'order_in_folder': order,
                        'predecessor': predecessor,
                        'column_index': pos_xy[0] if ('rankPosXY' in nested_info or 'rankPosXY' in item_info) else column_index,
                        'row_index': pos_xy[1] if ('rankPosXY' in nested_info or 'rankPosXY' in item_info) else nested_rank - 1
                    }
                    
                    results.append(nested_item)
                    self.log(f"          ОТЛАДКА: Добавлен элемент группы {nested_name} с предшественником '{predecessor}'", 'debug')
                
                # После группы следующий элемент должен зависеть от группы
                if group_item and not self.is_slave_unit(item_info):
                    next_should_depend_on_group = item_name
                    self.log(f"        ОТЛАДКА: Установлен флаг next_should_depend_on_group={item_name}", 'debug')
                    
            else:
                # Обычная техника
                rank = item_info.get('rank', 1)
                
                # Получаем координаты из rankPosXY если есть
                pos_xy = item_info.get('rankPosXY', [column_index, rank - 1])
                
                # Определяем предшественника
                predecessor = ''
                # Если поле reqAir отсутствует или не равно "", то есть зависимость
                has_dependency = 'reqAir' not in item_info or item_info.get('reqAir') != ''
                self.log(f"      ОТЛАДКА: Техника {item_name}, reqAir в JSON: {'есть' if 'reqAir' in item_info else 'нет'}, значение: '{item_info.get('reqAir', 'отсутствует')}', next_should_depend_on_group={next_should_depend_on_group}, has_dependency: {has_dependency}", 'debug')
                
                if has_dependency:
                    # Проверяем, должен ли этот элемент зависеть от группы
                    if next_should_depend_on_group:
                        predecessor = next_should_depend_on_group
                        next_should_depend_on_group = None
                        self.log(f"      ОТЛАДКА: Техника {item_name} зависит от группы {predecessor}", 'debug')
                    elif results:
                        # Обычная зависимость от предыдущего элемента
                        predecessor = results[-1]['id']
                        self.log(f"      ОТЛАДКА: Техника {item_name} зависит от {predecessor}", 'debug')
                else:
                    self.log(f"      ОТЛАДКА: Техника {item_name} имеет reqAir='', предшественника нет", 'debug')
                    
                regular_item = {
                    'id': item_name,
                    'rank': rank,
                    'country': country,
                    'vehicle_type': vehicle_type,
                    'type': 'vehicle',
                    'reqAir': item_info.get('reqAir', None),  # Сохраняем оригинальное значение
                    'is_group': False,
                    'order_in_folder': None,
                    'predecessor': predecessor,
                    'column_index': pos_xy[0] if 'rankPosXY' in item_info else column_index,
                    'row_index': pos_xy[1] if 'rankPosXY' in item_info else rank - 1
                }
                
                results.append(regular_item)
                self.log(f"      ОТЛАДКА: Добавлена техника {item_name} с предшественником '{predecessor}'", 'debug')
        
        return results

    def process_country_data(self, country_data: Dict[str, Any], country: str) -> List[Dict[str, Any]]:
        """Обрабатывает данные одной страны"""
        results = []
        
        for vehicle_type, type_data in country_data.items():
            if vehicle_type not in self.vehicle_type_mapping:
                continue
                
            vehicle_type_name = self.vehicle_type_mapping[vehicle_type]
            range_data = type_data.get('range', [])
            
            self.log(f"  Обработка типа: {vehicle_type_name}, столбцов: {len(range_data)}")
            
            # Обрабатываем каждый столбец (range)
            for column_index, column_data in enumerate(range_data):
                if not isinstance(column_data, dict):
                    self.log(f"    ОШИБКА: Столбец {column_index} не является словарем: {type(column_data)}", 'error')
                    continue
                    
                column_results = self.process_range_column(
                    column_data, country, vehicle_type_name, column_index
                )
                results.extend(column_results)
                self.log(f"    Столбец {column_index}: обработано {len(column_results)} элементов")
        
        return results

    def parse_shop_data(self, shop_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Основной метод парсинга данных shop.blkx"""
        all_results = []
        
        for country_key, country_data in shop_data.items():
            if country_key not in self.country_mapping:
                continue
                
            country_name = self.country_mapping[country_key]
            self.log(f"Обработка страны: {country_name}")
            
            country_results = self.process_country_data(country_data, country_name)
            all_results.extend(country_results)
            
            self.log(f"Обработано {len(country_results)} элементов для {country_name}")
        
        return all_results

    def save_to_csv(self, data: List[Dict[str, Any]], filename: str = 'shop.csv'):
        """Сохраняет данные в CSV файл"""
        if not data:
            self.log("Нет данных для сохранения", 'warning')
            return
            
        fieldnames = [
            'id', 'rank', 'country', 'vehicle_type', 'type',
            'column_index', 'row_index', 'predecessor',
            'order_in_folder'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                
                for item in data:
                    # Подготавливаем данные для записи
                    row_data = {
                        'id': item['id'],
                        'rank': item['rank'],
                        'country': item['country'],
                        'vehicle_type': item['vehicle_type'],
                        'type': item['type'],
                        'column_index': item['column_index'],
                        'row_index': item['row_index'],
                        'predecessor': item['predecessor'],
                        'order_in_folder': item.get('order_in_folder', '')
                    }
                    writer.writerow(row_data)
                    
            self.log(f"Данные ({len(data)} записей) сохранены в {filename}")
            
        except Exception as e:
            self.log(f"Ошибка при сохранении в CSV: {e}", 'error')
            raise

    def run(self):
        """Основной метод запуска парсера"""
        try:
            self.log("Запуск парсера shop.blkx...")
            
            # Загружаем данные
            shop_data = self.fetch_shop_data()
            
            # Парсим данные
            parsed_data = self.parse_shop_data(shop_data)
            
            # Сохраняем в CSV
            self.save_to_csv(parsed_data)
            
            self.log(f"Парсинг завершен успешно! Обработано {len(parsed_data)} элементов.")
            
        except Exception as e:
            self.log(f"Ошибка при выполнении парсинга: {e}", 'error')
            raise


def main():
    """Точка входа в программу"""
    try:
        parser = ShopParser()
        parser.run()
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()