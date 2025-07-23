import json
import csv
import requests
from typing import Dict, List, Any, Optional, Set, Tuple
import sys
import os
import logging

class ShopParser:
    # Порог для определения премиумной колонки (50% премиумной техники)
    PREMIUM_THRESHOLD = 0.5
    
    # Обрабатывать ли slave-юниты (для будущего развития)
    PROCESS_SLAVE_UNITS = False
    
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
        
        # Кэш для master-slave пар
        self.master_slave_pairs: Dict[str, str] = {}  # master_id -> slave_id
        self.slave_units: Set[str] = set()  # множество всех slave-юнитов
        
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

    def collect_master_slave_pairs(self, shop_data: Dict[str, Any]):
        """Собирает все master-slave пары перед основной обработкой"""
        self.log("Сбор master-slave пар...")
        
        for country_key, country_data in shop_data.items():
            if country_key not in self.country_mapping:
                continue
                
            for vehicle_type, type_data in country_data.items():
                if vehicle_type not in self.vehicle_type_mapping:
                    continue
                    
                range_data = type_data.get('range', [])
                
                for column_data in range_data:
                    if not isinstance(column_data, dict):
                        continue
                        
                    self._collect_pairs_from_column(column_data)
        
        self.log(f"Найдено master-slave пар: {len(self.master_slave_pairs)}")
        self.log(f"Slave-юниты: {self.slave_units}")
        
        for master, slave in self.master_slave_pairs.items():
            self.log(f"  {master} -> {slave}", 'debug')

    def _collect_pairs_from_column(self, column_data: Dict[str, Any]):
        """Собирает master-slave пары из одного столбца"""
        for item_name, item_info in column_data.items():
            if not isinstance(item_info, dict):
                continue
                
            # Проверяем master-slave пару
            if 'slaveUnit' in item_info:
                slave_id = item_info['slaveUnit']
                self.master_slave_pairs[item_name] = slave_id
                self.slave_units.add(slave_id)
                self.log(f"    Найдена пара: {item_name} -> {slave_id}", 'debug')
                
            # Проверяем элементы групп
            if self.is_group(item_name, item_info):
                group_items = self.get_group_items(item_info)
                for _, nested_name, nested_info in group_items:
                    if isinstance(nested_info, dict) and 'slaveUnit' in nested_info:
                        slave_id = nested_info['slaveUnit']
                        self.master_slave_pairs[nested_name] = slave_id
                        self.slave_units.add(slave_id)
                        self.log(f"    Найдена пара в группе: {nested_name} -> {slave_id}", 'debug')

    def is_premium_vehicle(self, item_data: Dict[str, Any]) -> bool:
        """Определяет является ли техника премиумной"""
        # Признаки премиумной техники
        premium_indicators = [
            'showOnlyWhenBought',
            'gift',
            'marketplaceItemdefId',
            'isClanVehicle',
            'showOnlyWhenResearch',  # Удаленная техника
            'event',
            'hideFeature',
            'beginPurchaseDate',
            'endPurchaseDate',
            'hideByPlatform'
        ]
        
        return any(indicator in item_data for indicator in premium_indicators)

    def is_premium_column(self, column_data: Dict[str, Any]) -> bool:
        """Определяет является ли колонка премиумной"""
        # Считаем количество премиумной техники в колонке
        total_items = 0
        premium_items = 0
        
        for item_name, item_info in column_data.items():
            if not isinstance(item_info, dict):
                continue
                
            # Пропускаем slave-юниты если они найдены отдельно
            if not self.PROCESS_SLAVE_UNITS and item_name in self.slave_units:
                continue
                
            total_items += 1
            if self.is_premium_vehicle(item_info):
                premium_items += 1
            
            # Проверяем элементы внутри групп
            if self.is_group(item_name, item_info):
                group_items = self.get_group_items(item_info)
                for _, nested_name, nested_info in group_items:
                    if isinstance(nested_info, dict):
                        # Пропускаем slave-юниты если они найдены отдельно
                        if not self.PROCESS_SLAVE_UNITS and nested_name in self.slave_units:
                            continue
                            
                        total_items += 1
                        if self.is_premium_vehicle(nested_info):
                            premium_items += 1
        
        # Если больше установленного порога техники премиумная, считаем колонку премиумной
        if total_items > 0:
            premium_ratio = premium_items / total_items
            self.log(f"      ОТЛАДКА: Премиум техники в колонке: {premium_items}/{total_items} = {premium_ratio:.2f}, порог: {self.PREMIUM_THRESHOLD}", 'debug')
            return premium_ratio >= self.PREMIUM_THRESHOLD
        
        return False

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
            'fakeReqUnitImage', 'fakeReqUnitRank', 'fakeReqUnitPosXY', 'showOnlyWhenResearch',
            'hideByPlatform'
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
            'fakeReqUnitImage', 'fakeReqUnitRank', 'fakeReqUnitPosXY', 'showOnlyWhenResearch',
            'hideByPlatform'
        }
        items = []
        
        order = 0
        for item_name, item_data in group_data.items():
            if item_name not in service_fields:
                # Добавляем отладочную информацию
                if not isinstance(item_data, dict):
                    self.log(f"        ОТЛАДКА: Элемент группы {item_name} имеет тип {type(item_data)}: {item_data}", 'debug')
                items.append((order, item_name, item_data))
                self.log(f"        ОТЛАДКА: get_group_items добавлен {item_name} с order={order}", 'debug')
                order += 1
                
        return items

    def calculate_premium_coordinates(self, items: List[Dict[str, Any]], premium_column_index: int) -> List[Dict[str, Any]]:
        """Вычисляет координаты для премиумной техники с группировкой по рангам"""
        # Группируем элементы по рангам
        items_by_rank: Dict[int, List[Dict[str, Any]]] = {}
        
        for item in items:
            rank = item['rank']
            if rank not in items_by_rank:
                items_by_rank[rank] = []
            items_by_rank[rank].append(item)
        
        # Назначаем координаты внутри каждого ранга
        for rank in sorted(items_by_rank.keys()):
            rank_items = items_by_rank[rank]
            
            for row_index, item in enumerate(rank_items):
                item['column_index'] = premium_column_index
                item['row_index'] = row_index
                self.log(f"        ПРЕМИУМ КООРДИНАТЫ: {item['id']} ранг {rank} -> [{premium_column_index}, {row_index}]", 'debug')
        
        return items

    def process_range_column(self, range_data: Dict[str, Any], country: str, 
                           vehicle_type: str, column_index: int, is_premium: bool = False) -> List[Dict[str, Any]]:
        """Обрабатывает один столбец (range) техники"""
        results = []
        next_should_depend_on_group = None
        
        # Счетчик для премиумных колонок
        premium_column_index = 0
        
        # Обрабатываем элементы в том порядке, в котором они идут в JSON
        for item_name, item_info in range_data.items():
            if not isinstance(item_info, dict):
                self.log(f"      ПРЕДУПРЕЖДЕНИЕ: Элемент {item_name} не является словарем: {type(item_info)}", 'warning')
                continue
            
            # Пропускаем slave-юниты если настройка отключена
            if not self.PROCESS_SLAVE_UNITS and item_name in self.slave_units:
                self.log(f"      ПРОПУСК: {item_name} является slave-юнитом", 'debug')
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
                    # Для обычной техники используем обычные координаты
                    if not is_premium:
                        # Получаем координаты из rankPosXY если есть
                        pos_xy = item_info.get('rankPosXY', [column_index, actual_rank - 1])
                    else:
                        # Для премиум техники координаты назначим позже
                        pos_xy = [0, 0]  # Временные значения
                    
                    # Определяем предшественника для группы
                    predecessor = ''
                    # Если поле reqAir отсутствует или не равно "", то есть зависимость
                    has_dependency = 'reqAir' not in item_info or item_info.get('reqAir') != ''
                    self.log(f"        ОТЛАДКА: Группа {item_name}, reqAir в JSON: {'есть' if 'reqAir' in item_info else 'нет'}, значение: '{item_info.get('reqAir', 'отсутствует')}', has_dependency: {has_dependency}", 'debug')
                    
                    if has_dependency and not is_premium:
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
                        'status': 'premium' if is_premium else 'standard',
                        'reqAir': item_info.get('reqAir', None),  # Сохраняем оригинальное значение
                        'is_group': True,
                        'order_in_folder': None,
                        'predecessor': predecessor if not is_premium else '',
                        'column_index': pos_xy[0],
                        'row_index': pos_xy[1]
                    }
                    
                    results.append(group_item)
                    self.log(f"        ОТЛАДКА: Добавлена группа {item_name} с предшественником '{predecessor}', статус: {group_item['status']}", 'debug')
                
                # Обрабатываем элементы внутри группы
                for order, nested_name, nested_info in group_items:
                    if not isinstance(nested_info, dict):
                        self.log(f"        ПРЕДУПРЕЖДЕНИЕ: Элемент группы {nested_name} не является словарем: {type(nested_info)} = {nested_info}", 'warning')
                        continue
                    
                    # Пропускаем slave-юниты если настройка отключена
                    if not self.PROCESS_SLAVE_UNITS and nested_name in self.slave_units:
                        self.log(f"        ПРОПУСК: {nested_name} является slave-юнитом в группе", 'debug')
                        continue
                        
                    nested_rank = nested_info.get('rank', actual_rank)
                    
                    # Для slave-unit берем только основной элемент
                    if self.is_slave_unit(item_info):
                        parent_id = item_info.get('slaveUnit', '')
                    else:
                        parent_id = item_name
                    
                    # Для обычной техники используем обычные координаты
                    if not is_premium:
                        # Получаем координаты из rankPosXY если есть
                        pos_xy = nested_info.get('rankPosXY', item_info.get('rankPosXY', [column_index, nested_rank - 1]))
                    else:
                        # Для премиум техники координаты назначим позже
                        pos_xy = [0, 0]  # Временные значения
                    
                    # Определяем предшественника для элемента группы
                    predecessor = ''
                    # Если поле reqAir отсутствует или не равно "", то есть зависимость
                    has_dependency = 'reqAir' not in nested_info or nested_info.get('reqAir') != ''
                    self.log(f"          ОТЛАДКА: Элемент {nested_name}, reqAir в JSON: {'есть' if 'reqAir' in nested_info else 'нет'}, значение: '{nested_info.get('reqAir', 'отсутствует')}', order={order}, parent_id={parent_id}, has_dependency: {has_dependency}", 'debug')
                    
                    if has_dependency and not is_premium:
                        # Для первого элемента группы (order=0)
                        if order == 0:
                            # Первый элемент группы зависит от самой группы
                            predecessor = parent_id
                            self.log(f"          ОТЛАДКА: Первый элемент группы {nested_name} (order={order}) зависит от группы {parent_id}", 'debug')
                        else:
                            # Остальные элементы зависят от предыдущего элемента группы
                            prev_order = order - 1
                            self.log(f"          ОТЛАДКА: Ищем предшественника для {nested_name} с prev_order={prev_order}", 'debug')
                            for prev_order_item, prev_name, prev_info in group_items:
                                self.log(f"          ОТЛАДКА: Проверяем элемент {prev_name} с order={prev_order_item}", 'debug')
                                if prev_order_item == prev_order:
                                    # Проверяем, не является ли предыдущий элемент slave-юнитом
                                    if not self.PROCESS_SLAVE_UNITS and prev_name in self.slave_units:
                                        continue  # Ищем дальше
                                    predecessor = prev_name
                                    self.log(f"          ОТЛАДКА: Элемент группы {nested_name} зависит от {prev_name}", 'debug')
                                    break
                            
                            if not predecessor:
                                self.log(f"          ОТЛАДКА: ОШИБКА! Не найден предшественник для {nested_name} с prev_order={prev_order}", 'debug')
                    else:
                        self.log(f"          ОТЛАДКА: Элемент группы {nested_name} имеет reqAir='', предшественника нет", 'debug')
                    
                    self.log(f"          ОТЛАДКА: Финальный predecessor для {nested_name}: '{predecessor}'", 'debug')
                        
                    nested_item = {
                        'id': nested_name,
                        'rank': nested_rank,
                        'country': country,
                        'vehicle_type': vehicle_type,
                        'type': 'vehicle',
                        'status': 'premium' if is_premium else 'standard',
                        'reqAir': nested_info.get('reqAir', None),  # Сохраняем оригинальное значение
                        'is_group': False,
                        'parent_id': parent_id,
                        'order_in_folder': order,
                        'predecessor': predecessor if not is_premium else '',
                        'column_index': pos_xy[0],
                        'row_index': pos_xy[1]
                    }
                    
                    results.append(nested_item)
                    self.log(f"          ОТЛАДКА: Добавлен элемент группы {nested_name} с предшественником '{predecessor}', статус: {nested_item['status']}", 'debug')
                
                # После группы следующий элемент должен зависеть от группы
                if group_item and not self.is_slave_unit(item_info) and not is_premium:
                    next_should_depend_on_group = item_name
                    self.log(f"        ОТЛАДКА: Установлен флаг next_should_depend_on_group={item_name}", 'debug')
                    
            else:
                # Обычная техника
                rank = item_info.get('rank', 1)
                
                # Для обычной техники используем обычные координаты
                if not is_premium:
                    # Получаем координаты из rankPosXY если есть
                    pos_xy = item_info.get('rankPosXY', [column_index, rank - 1])
                else:
                    # Для премиум техники координаты назначим позже
                    pos_xy = [0, 0]  # Временные значения
                
                # Определяем предшественника
                predecessor = ''
                # Если поле reqAir отсутствует или не равно "", то есть зависимость
                has_dependency = 'reqAir' not in item_info or item_info.get('reqAir') != ''
                self.log(f"      ОТЛАДКА: Техника {item_name}, reqAir в JSON: {'есть' if 'reqAir' in item_info else 'нет'}, значение: '{item_info.get('reqAir', 'отсутствует')}', next_should_depend_on_group={next_should_depend_on_group}, has_dependency: {has_dependency}", 'debug')
                
                if has_dependency and not is_premium:
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
                    'status': 'premium' if is_premium else 'standard',
                    'reqAir': item_info.get('reqAir', None),  # Сохраняем оригинальное значение
                    'is_group': False,
                    'order_in_folder': None,
                    'predecessor': predecessor if not is_premium else '',
                    'column_index': pos_xy[0],
                    'row_index': pos_xy[1]
                }
                
                results.append(regular_item)
                self.log(f"      ОТЛАДКА: Добавлена техника {item_name} с предшественником '{predecessor}', статус: {regular_item['status']}", 'debug')
        
        # Если это премиумная колонка, пересчитываем координаты
        if is_premium:
            results = self.calculate_premium_coordinates(results, premium_column_index)
        
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
            
            # Счетчик премиумных колонок для правильного определения column_index
            premium_column_count = 0
            
            # Обрабатываем каждый столбец (range)
            for column_index, column_data in enumerate(range_data):
                if not isinstance(column_data, dict):
                    self.log(f"    ОШИБКА: Столбец {column_index} не является словарем: {type(column_data)}", 'error')
                    continue
                
                # Определяем, является ли колонка премиумной
                is_premium = self.is_premium_column(column_data)
                
                if is_premium:
                    self.log(f"    Столбец {column_index} определен как ПРЕМИУМНЫЙ (порог: {self.PREMIUM_THRESHOLD*100}%)")
                    # Для премиумных колонок используем отдельный счетчик
                    effective_column_index = premium_column_count
                    premium_column_count += 1
                else:
                    effective_column_index = column_index
                
                column_results = self.process_range_column(
                    column_data, country, vehicle_type_name, effective_column_index, is_premium
                )
                results.extend(column_results)
                self.log(f"    Столбец {column_index}: обработано {len(column_results)} элементов")
        
        return results

    def parse_shop_data(self, shop_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Основной метод парсинга данных shop.blkx"""
        # Сначала собираем все master-slave пары
        self.collect_master_slave_pairs(shop_data)
        
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
            'id', 'rank', 'country', 'vehicle_type', 'type', 'status',
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
                        'status': item['status'],
                        'column_index': item['column_index'],
                        'row_index': item['row_index'],
                        'predecessor': item['predecessor'],
                        'order_in_folder': item.get('order_in_folder', '')
                    }
                    writer.writerow(row_data)
                    
            self.log(f"Данные ({len(data)} записей) сохранены в {filename}")
            
            # Дополнительная статистика
            premium_count = sum(1 for item in data if item['status'] == 'premium')
            standard_count = len(data) - premium_count
            self.log(f"Статистика: Стандартной техники: {standard_count}, Премиумной: {premium_count}")
            
            # Статистика по master-slave парам
            if self.master_slave_pairs:
                self.log(f"Обработано master-slave пар: {len(self.master_slave_pairs)}")
                if not self.PROCESS_SLAVE_UNITS:
                    self.log(f"Slave-юниты пропущены: {len(self.slave_units)}")
            
        except Exception as e:
            self.log(f"Ошибка при сохранении в CSV: {e}", 'error')
            raise

    def run(self):
        """Основной метод запуска парсера"""
        try:
            self.log("Запуск парсера shop.blkx...")
            self.log(f"Порог для определения премиумных колонок: {self.PREMIUM_THRESHOLD*100}%")
            self.log(f"Обработка slave-юнитов: {'включена' if self.PROCESS_SLAVE_UNITS else 'отключена'}")
            
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