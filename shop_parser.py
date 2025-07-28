import json
import csv
import requests
from typing import Dict, List, Any, Optional, Set, Tuple

from utils import Config, Logger, Constants


class ShopParser:
    """Основной класс для парсинга данных shop.blkx"""
    
    def __init__(self, config_path: str = 'config.txt'):
        """
        Инициализация парсера shop.blkx
        
        Args:
            config_path: Путь к конфигурационному файлу
        """
        self.config = Config(config_path)
        self.logger = Logger()
        self.vehicles_data = []
        
        # Кэш для master-slave пар
        self.master_slave_pairs: Dict[str, str] = {}  # master_id -> slave_id
        self.slave_units: Set[str] = set()  # множество всех slave-юнитов
        self.image_fields: Dict[str, str] = {}

    def fetch_shop_data(self) -> Dict[str, Any]:
        """Загружает данные shop.blkx из источника"""
        shop_url = self.config.get('shop_url')
        if not shop_url:
            raise ValueError("shop_url не найден в конфигурации")
            
        try:
            self.logger.log(f"Загрузка данных из: {shop_url}")
            response = requests.get(shop_url, timeout=30)
            response.raise_for_status()
            
            shop_data = response.json()
            self.logger.log(f"Данные успешно загружены")
            return shop_data
            
        except requests.RequestException as e:
            raise RuntimeError(f"Ошибка загрузки данных: {e}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Ошибка декодирования JSON: {e}")

    def has_anomalous_suffix(self, element_id: str) -> bool:
        """Проверяет, имеет ли элемент аномальное окончание или начало"""
        # Проверка окончаний (существующая логика)
        has_suffix = any(element_id.endswith(suffix) for suffix in Constants.ANOMALOUS_SUFFIXES)
        
        # Проверка начал (новая логика)
        has_prefix = any(element_id.startswith(prefix) for prefix in Constants.ANOMALOUS_PREFIXES)
        
        return has_suffix or has_prefix

    def clean_anomalous_elements(self, shop_data: Dict[str, Any]) -> Dict[str, Any]:
        """Удаляет аномальные элементы из данных shop.blkx перед парсингом"""
        self.logger.log("Начинаем очистку от аномальных элементов...")
        
        cleaned_data = {}
        total_removed = 0
        removed_details = []
        
        for country_key, country_data in shop_data.items():
            if country_key not in Constants.COUNTRY_MAPPING:
                cleaned_data[country_key] = country_data
                continue
                
            country_name = Constants.COUNTRY_MAPPING[country_key]
            self.logger.log(f"  Очистка страны: {country_name}")
            
            cleaned_country = {}
            
            for vehicle_type, type_data in country_data.items():
                if vehicle_type not in Constants.VEHICLE_TYPE_MAPPING:
                    cleaned_country[vehicle_type] = type_data
                    continue
                    
                vehicle_type_name = Constants.VEHICLE_TYPE_MAPPING[vehicle_type]
                range_data = type_data.get('range', [])
                
                cleaned_ranges = []
                
                for column_index, column_data in enumerate(range_data):
                    if not isinstance(column_data, dict):
                        cleaned_ranges.append(column_data)
                        continue
                    
                    cleaned_column = {}
                    
                    for item_name, item_info in column_data.items():
                        # Проверяем аномальность основного элемента
                        if self.has_anomalous_suffix(item_name):
                            total_removed += 1
                            reason = "аномальное окончание/начало"
                            removed_details.append(f"{country_name}/{vehicle_type_name}/column_{column_index}/{item_name} ({reason})")
                            self.logger.log(f"    УДАЛЕН: {item_name} ({reason})", 'debug')
                            continue
                        
                        # Если элемент - группа, проверяем вложенные элементы
                        if isinstance(item_info, dict) and self.is_group(item_name, item_info):
                            has_anomalous_children = False
                            
                            # Проверяем все вложенные элементы на аномальность
                            for nested_key in item_info.keys():
                                if nested_key not in Constants.SERVICE_FIELDS:
                                    if self.has_anomalous_suffix(nested_key):
                                        has_anomalous_children = True
                                        self.logger.log(f"      Найден аномальный элемент в группе: {nested_key}", 'debug')
                                        break
                            
                            if has_anomalous_children:
                                total_removed += 1
                                reason = "группа с аномальными элементами"
                                removed_details.append(f"{country_name}/{vehicle_type_name}/column_{column_index}/{item_name} ({reason})")
                                self.logger.log(f"    УДАЛЕНА ГРУППА: {item_name} ({reason})", 'debug')
                                continue
                        
                        # Элемент прошел проверку - добавляем в очищенные данные
                        cleaned_column[item_name] = item_info
                    
                    cleaned_ranges.append(cleaned_column)
                
                cleaned_country[vehicle_type] = {'range': cleaned_ranges}
            
            cleaned_data[country_key] = cleaned_country
        
        # Логируем результаты очистки
        self.logger.log(f"Очистка завершена. Удалено элементов: {total_removed}")
        
        if removed_details:
            self.logger.log("Подробный список удаленных элементов:")
            for detail in removed_details:
                self.logger.log(f"  - {detail}")
        
        if total_removed == 0:
            self.logger.log("Аномальные элементы не найдены")
        
        return cleaned_data

    def collect_master_slave_pairs(self, shop_data: Dict[str, Any]):
        """Собирает все master-slave пары перед основной обработкой"""
        self.logger.log("Сбор master-slave пар...")
        
        for country_key, country_data in shop_data.items():
            if country_key not in Constants.COUNTRY_MAPPING:
                continue
                
            for vehicle_type, type_data in country_data.items():
                if vehicle_type not in Constants.VEHICLE_TYPE_MAPPING:
                    continue
                    
                range_data = type_data.get('range', [])
                
                for column_data in range_data:
                    if not isinstance(column_data, dict):
                        continue
                        
                    self._collect_pairs_from_column(column_data)
        
        self.logger.log(f"Найдено master-slave пар: {len(self.master_slave_pairs)}")
        self.logger.log(f"Slave-юниты: {self.slave_units}")
        
        for master, slave in self.master_slave_pairs.items():
            self.logger.log(f"  {master} -> {slave}", 'debug')

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
                self.logger.log(f"    Найдена пара: {item_name} -> {slave_id}", 'debug')
                
            # Проверяем элементы групп
            if self.is_group(item_name, item_info):
                group_items = self.get_group_items(item_info)
                for _, nested_name, nested_info in group_items:
                    if isinstance(nested_info, dict) and 'slaveUnit' in nested_info:
                        slave_id = nested_info['slaveUnit']
                        self.master_slave_pairs[nested_name] = slave_id
                        self.slave_units.add(slave_id)
                        self.logger.log(f"    Найдена пара в группе: {nested_name} -> {slave_id}", 'debug')

    def _extract_image_field(self, item_name: str, item_info: Dict[str, Any]):
        """Извлекает поле image из данных юнита"""
        if isinstance(item_info, dict) and 'image' in item_info:
            image_path = item_info['image']
            # Форматируем: берем текст после последнего #
            if '#' in image_path:
                formatted_image = image_path.split('#')[-1]
                self.image_fields[item_name.lower()] = formatted_image
                self.logger.log(f"  Найдено поле image: {item_name} -> {formatted_image}", 'debug')

    def extract_all_image_fields(self, shop_data: Dict[str, Any]):
        """Извлекает все поля image из данных shop.blkx"""
        self.logger.log("Извлечение полей image...")
        
        for country_key, country_data in shop_data.items():
            if country_key not in Constants.COUNTRY_MAPPING:
                continue
                
            for vehicle_type, type_data in country_data.items():
                if vehicle_type not in Constants.VEHICLE_TYPE_MAPPING:
                    continue
                    
                range_data = type_data.get('range', [])
                
                for column_data in range_data:
                    if not isinstance(column_data, dict):
                        continue
                        
                    for item_name, item_info in column_data.items():
                        if not isinstance(item_info, dict):
                            continue
                            
                        # Извлекаем image для основного элемента
                        self._extract_image_field(item_name, item_info)
                        
                        # Если это группа, извлекаем image для вложенных элементов
                        if self.is_group(item_name, item_info):
                            group_items = self.get_group_items(item_info)
                            for _, nested_name, nested_info in group_items:
                                if isinstance(nested_info, dict):
                                    self._extract_image_field(nested_name, nested_info)
        
        self.logger.log(f"Извлечено полей image: {len(self.image_fields)}")

    def is_premium_vehicle(self, item_data: Dict[str, Any]) -> bool:
        """Определяет является ли техника премиумной"""
        return any(indicator in item_data for indicator in Constants.PREMIUM_INDICATORS)

    def is_premium_column(self, column_data: Dict[str, Any]) -> bool:
        """Определяет является ли колонка премиумной"""
        # Считаем количество премиумной техники в колонке
        total_items = 0
        premium_items = 0
        
        for item_name, item_info in column_data.items():
            if not isinstance(item_info, dict):
                continue
                
            # Пропускаем slave-юниты если они найдены отдельно
            if not Constants.PROCESS_SLAVE_UNITS and item_name in self.slave_units:
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
                        if not Constants.PROCESS_SLAVE_UNITS and nested_name in self.slave_units:
                            continue
                            
                        total_items += 1
                        if self.is_premium_vehicle(nested_info):
                            premium_items += 1
        
        # Если больше установленного порога техники премиумная, считаем колонку премиумной
        if total_items > 0:
            premium_ratio = premium_items / total_items
            self.logger.log(f"      ОТЛАДКА: Премиум техники в колонке: {premium_items}/{total_items} = {premium_ratio:.2f}, порог: {Constants.PREMIUM_THRESHOLD}", 'debug')
            return premium_ratio >= Constants.PREMIUM_THRESHOLD
        
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
        nested_items = [key for key in item_data.keys() if key not in Constants.SERVICE_FIELDS]
        
        return len(nested_items) > 0

    def get_group_items(self, group_data: Dict[str, Any]) -> List[tuple]:
        """Извлекает элементы из группы с их порядковыми номерами"""
        items = []
        
        order = 0
        for item_name, item_data in group_data.items():
            if item_name not in Constants.SERVICE_FIELDS:
                # Добавляем отладочную информацию
                if not isinstance(item_data, dict):
                    self.logger.log(f"        ОТЛАДКА: Элемент группы {item_name} имеет тип {type(item_data)}: {item_data}", 'debug')
                items.append((order, item_name, item_data))
                self.logger.log(f"        ОТЛАДКА: get_group_items добавлен {item_name} с order={order}", 'debug')
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
            
            # Группируем элементы по группам для правильного назначения координат
            groups_in_rank = {}
            standalone_items = []
            
            for item in rank_items:
                if item.get('parent_id'):
                    # Элемент группы - добавляем к группе
                    parent_id = item['parent_id']
                    if parent_id not in groups_in_rank:
                        groups_in_rank[parent_id] = {'group': None, 'children': []}
                    groups_in_rank[parent_id]['children'].append(item)
                elif item.get('is_group'):
                    # Сама группа
                    group_id = item['id']
                    if group_id not in groups_in_rank:
                        groups_in_rank[group_id] = {'group': None, 'children': []}
                    groups_in_rank[group_id]['group'] = item
                else:
                    # Отдельный элемент
                    standalone_items.append(item)
            
            # Назначаем координаты
            row_index = 0
            
            # Сначала обрабатываем отдельные элементы
            for item in standalone_items:
                item['column_index'] = premium_column_index
                item['row_index'] = row_index
                self.logger.log(f"        ПРЕМИУМ КООРДИНАТЫ: {item['id']} ранг {rank} -> [{premium_column_index}, {row_index}]", 'debug')
                row_index += 1
            
            # Затем обрабатываем группы
            for group_id, group_data in groups_in_rank.items():
                group_item = group_data['group']
                children = group_data['children']
                
                if group_item:
                    # Назначаем координаты группе
                    group_item['column_index'] = premium_column_index
                    group_item['row_index'] = row_index
                    self.logger.log(f"        ПРЕМИУМ КООРДИНАТЫ: {group_item['id']} ранг {rank} -> [{premium_column_index}, {row_index}]", 'debug')
                    
                    # Все элементы группы получают те же координаты что и группа
                    for child in children:
                        child['column_index'] = premium_column_index
                        child['row_index'] = row_index
                        self.logger.log(f"        ПРЕМИУМ КООРДИНАТЫ: {child['id']} ранг {rank} -> [{premium_column_index}, {row_index}] (элемент группы)", 'debug')
                    
                    row_index += 1
                else:
                    # Группа не найдена, но есть элементы - обрабатываем как отдельные
                    for child in children:
                        child['column_index'] = premium_column_index
                        child['row_index'] = row_index
                        self.logger.log(f"        ПРЕМИУМ КООРДИНАТЫ: {child['id']} ранг {rank} -> [{premium_column_index}, {row_index}] (без группы)", 'debug')
                        row_index += 1
        
        return items

    def process_range_column(self, range_data: Dict[str, Any], country: str, 
                           vehicle_type: str, premium_column_index: int, is_premium: bool = False) -> List[Dict[str, Any]]:
        """Обрабатывает один столбец (range) техники"""
        results = []
        next_should_depend_on_group = None
        
        # Обрабатываем элементы в том порядке, в котором они идут в JSON
        for item_name, item_info in range_data.items():
            if not isinstance(item_info, dict):
                self.logger.log(f"      ПРЕДУПРЕЖДЕНИЕ: Элемент {item_name} не является словарем: {type(item_info)}", 'warning')
                continue
            
            # Пропускаем slave-юниты если настройка отключена
            if not Constants.PROCESS_SLAVE_UNITS and item_name in self.slave_units:
                self.logger.log(f"      ПРОПУСК: {item_name} является slave-юнитом", 'debug')
                continue
                
            if self.is_group(item_name, item_info):
                # Обрабатываем группу
                results.extend(self._process_group(item_name, item_info, country, vehicle_type, 
                                                 premium_column_index, is_premium, results, 
                                                 next_should_depend_on_group))
                
                # После группы следующий элемент должен зависеть от группы
                if not (item_name in self.slave_units and not Constants.PROCESS_SLAVE_UNITS) and not is_premium:
                    next_should_depend_on_group = item_name
                    self.logger.log(f"        ОТЛАДКА: Установлен флаг next_should_depend_on_group={item_name}", 'debug')
                    
            else:
                # Обрабатываем обычную технику
                regular_item = self._process_regular_item(item_name, item_info, country, vehicle_type,
                                                        premium_column_index, is_premium, results,
                                                        next_should_depend_on_group)
                if regular_item:
                    results.append(regular_item)
                    next_should_depend_on_group = None
        
        # Если это премиумная колонка, пересчитываем координаты с правильным column_index
        if is_premium:
            results = self.calculate_premium_coordinates(results, premium_column_index)
        
        return results

    def _process_group(self, item_name: str, item_info: Dict[str, Any], country: str, 
                      vehicle_type: str, premium_column_index: int, is_premium: bool,
                      results: List[Dict[str, Any]], next_should_depend_on_group: Optional[str]) -> List[Dict[str, Any]]:
        """Обрабатывает группу техники"""
        group_results = []
        rank = item_info.get('rank', 1)
        
        # Получаем элементы группы для определения правильного ранга
        group_items = self.get_group_items(item_info)
        if group_items:
            # Ранг группы = ранг её первого элемента
            first_item = group_items[0]
            first_item_info = first_item[2]
            
            if isinstance(first_item_info, dict):
                actual_rank = first_item_info.get('rank', rank)
            else:
                self.logger.log(f"        ПРЕДУПРЕЖДЕНИЕ: Первый элемент группы {item_name} не является словарем: {type(first_item_info)}", 'warning')
                actual_rank = rank
        else:
            actual_rank = rank
        
        # Добавляем саму группу (если не slave-unit)
        group_item = None
        if not (item_name in self.slave_units and not Constants.PROCESS_SLAVE_UNITS):
            group_item = self._create_group_item(item_name, item_info, country, vehicle_type,
                                               premium_column_index, is_premium, actual_rank,
                                               results, next_should_depend_on_group)
            if group_item:
                group_results.append(group_item)
        
        # Обрабатываем элементы внутри группы
        for order, nested_name, nested_info in group_items:
            if not isinstance(nested_info, dict):
                self.logger.log(f"        ПРЕДУПРЕЖДЕНИЕ: Элемент группы {nested_name} не является словарем: {type(nested_info)} = {nested_info}", 'warning')
                continue
            
            # Пропускаем slave-юниты если настройка отключена
            if not Constants.PROCESS_SLAVE_UNITS and nested_name in self.slave_units:
                self.logger.log(f"        ПРОПУСК: {nested_name} является slave-юнитом в группе", 'debug')
                continue
            
            nested_item = self._create_group_child_item(nested_name, nested_info, item_name, country,
                                                      vehicle_type, premium_column_index, is_premium,
                                                      actual_rank, order, group_items)
            if nested_item:
                group_results.append(nested_item)
        
        return group_results

    def _create_group_item(self, item_name: str, item_info: Dict[str, Any], country: str,
                          vehicle_type: str, premium_column_index: int, is_premium: bool,
                          actual_rank: int, results: List[Dict[str, Any]], 
                          next_should_depend_on_group: Optional[str]) -> Optional[Dict[str, Any]]:
        """Создает элемент группы"""
        # Для обычной техники используем обычные координаты
        if not is_premium:
            pos_xy = item_info.get('rankPosXY', [premium_column_index, actual_rank - 1])
        else:
            pos_xy = [0, 0]  # Временные значения
        
        # Определяем предшественника для группы
        predecessor = ''
        has_dependency = 'reqAir' not in item_info or item_info.get('reqAir') != ''
        self.logger.log(f"        ОТЛАДКА: Группа {item_name}, reqAir в JSON: {'есть' if 'reqAir' in item_info else 'нет'}, значение: '{item_info.get('reqAir', 'отсутствует')}', has_dependency: {has_dependency}", 'debug')
        
        if has_dependency and not is_premium:
            if next_should_depend_on_group:
                predecessor = next_should_depend_on_group
                self.logger.log(f"        ОТЛАДКА: Группа {item_name} зависит от предыдущей группы {predecessor}", 'debug')
            elif results:
                predecessor = results[-1]['id']
                self.logger.log(f"        ОТЛАДКА: Группа {item_name} зависит от {predecessor}", 'debug')
        else:
            self.logger.log(f"        ОТЛАДКА: Группа {item_name} имеет reqAir='', предшественника нет", 'debug')
        
        group_item = {
            'id': item_name,
            'rank': actual_rank,
            'country': country,
            'vehicle_type': vehicle_type,
            'type': 'folder',
            'status': 'premium' if is_premium else 'standard',
            'reqAir': item_info.get('reqAir', None),
            'is_group': True,
            'order_in_folder': None,
            'predecessor': predecessor if not is_premium else '',
            'column_index': pos_xy[0],
            'row_index': pos_xy[1]
        }
        
        self.logger.log(f"        ОТЛАДКА: Добавлена группа {item_name} с предшественником '{predecessor}', статус: {group_item['status']}", 'debug')
        return group_item

    def _create_group_child_item(self, nested_name: str, nested_info: Dict[str, Any], parent_name: str,
                               country: str, vehicle_type: str, premium_column_index: int, is_premium: bool,
                               actual_rank: int, order: int, group_items: List[tuple]) -> Optional[Dict[str, Any]]:
        """Создает элемент внутри группы"""
        nested_rank = nested_info.get('rank', actual_rank)
        
        # Для slave-unit берем только основной элемент
        parent_id = self.master_slave_pairs.get(parent_name, parent_name)
        
        # Для обычной техники используем обычные координаты
        if not is_premium:
            pos_xy = nested_info.get('rankPosXY', [premium_column_index, nested_rank - 1])
        else:
            pos_xy = [0, 0]  # Временные значения
        
        # Определяем предшественника для элемента группы
        predecessor = ''
        has_dependency = 'reqAir' not in nested_info or nested_info.get('reqAir') != ''
        self.logger.log(f"          ОТЛАДКА: Элемент {nested_name}, reqAir в JSON: {'есть' if 'reqAir' in nested_info else 'нет'}, значение: '{nested_info.get('reqAir', 'отсутствует')}', order={order}, parent_id={parent_id}, has_dependency: {has_dependency}", 'debug')
        
        if has_dependency and not is_premium:
            if order == 0:
                # Первый элемент группы зависит от самой группы
                predecessor = parent_id
                self.logger.log(f"          ОТЛАДКА: Первый элемент группы {nested_name} (order={order}) зависит от группы {parent_id}", 'debug')
            else:
                # Остальные элементы зависят от предыдущего элемента группы
                prev_order = order - 1
                for prev_order_item, prev_name, prev_info in group_items:
                    if prev_order_item == prev_order:
                        if not Constants.PROCESS_SLAVE_UNITS and prev_name in self.slave_units:
                            continue
                        predecessor = prev_name
                        self.logger.log(f"          ОТЛАДКА: Элемент группы {nested_name} зависит от {prev_name}", 'debug')
                        break
        else:
            self.logger.log(f"          ОТЛАДКА: Элемент группы {nested_name} имеет reqAir='', предшественника нет", 'debug')
        
        nested_item = {
            'id': nested_name,
            'rank': nested_rank,
            'country': country,
            'vehicle_type': vehicle_type,
            'type': 'vehicle',
            'status': 'premium' if is_premium else 'standard',
            'reqAir': nested_info.get('reqAir', None),
            'is_group': False,
            'parent_id': parent_id,
            'order_in_folder': order,
            'predecessor': predecessor if not is_premium else '',
            'column_index': pos_xy[0],
            'row_index': pos_xy[1]
        }
        
        self.logger.log(f"          ОТЛАДКА: Добавлен элемент группы {nested_name} с предшественником '{predecessor}', статус: {nested_item['status']}", 'debug')
        return nested_item

    def _process_regular_item(self, item_name: str, item_info: Dict[str, Any], country: str,
                            vehicle_type: str, premium_column_index: int, is_premium: bool,
                            results: List[Dict[str, Any]], next_should_depend_on_group: Optional[str]) -> Optional[Dict[str, Any]]:
        """Обрабатывает обычную технику"""
        rank = item_info.get('rank', 1)
        
        # Для обычной техники используем обычные координаты
        if not is_premium:
            pos_xy = item_info.get('rankPosXY', [premium_column_index, rank - 1])
        else:
            pos_xy = [0, 0]  # Временные значения
        
        # Определяем предшественника
        predecessor = ''
        has_dependency = 'reqAir' not in item_info or item_info.get('reqAir') != ''
        self.logger.log(f"      ОТЛАДКА: Техника {item_name}, reqAir в JSON: {'есть' if 'reqAir' in item_info else 'нет'}, значение: '{item_info.get('reqAir', 'отсутствует')}', next_should_depend_on_group={next_should_depend_on_group}, has_dependency: {has_dependency}", 'debug')
        
        if has_dependency and not is_premium:
            if next_should_depend_on_group:
                predecessor = next_should_depend_on_group
                self.logger.log(f"      ОТЛАДКА: Техника {item_name} зависит от группы {predecessor}", 'debug')
            elif results:
                predecessor = results[-1]['id']
                self.logger.log(f"      ОТЛАДКА: Техника {item_name} зависит от {predecessor}", 'debug')
        else:
            self.logger.log(f"      ОТЛАДКА: Техника {item_name} имеет reqAir='', предшественника нет", 'debug')
            
        regular_item = {
            'id': item_name,
            'rank': rank,
            'country': country,
            'vehicle_type': vehicle_type,
            'type': 'vehicle',
            'status': 'premium' if is_premium else 'standard',
            'reqAir': item_info.get('reqAir', None),
            'is_group': False,
            'order_in_folder': None,
            'predecessor': predecessor if not is_premium else '',
            'column_index': pos_xy[0],
            'row_index': pos_xy[1]
        }
        
        self.logger.log(f"      ОТЛАДКА: Добавлена техника {item_name} с предшественником '{predecessor}', статус: {regular_item['status']}", 'debug')
        return regular_item

    def process_country_data(self, country_data: Dict[str, Any], country: str) -> List[Dict[str, Any]]:
        """Обрабатывает данные одной страны"""
        results = []
        
        for vehicle_type, type_data in country_data.items():
            if vehicle_type not in Constants.VEHICLE_TYPE_MAPPING:
                continue
                
            vehicle_type_name = Constants.VEHICLE_TYPE_MAPPING[vehicle_type]
            range_data = type_data.get('range', [])
            
            self.logger.log(f"  Обработка типа: {vehicle_type_name}, столбцов: {len(range_data)}")
            
            # Счетчик премиумных колонок для правильного определения column_index
            premium_column_count = 0
            
            # Обрабатываем каждый столбец (range)
            for column_index, column_data in enumerate(range_data):
                if not isinstance(column_data, dict):
                    self.logger.log(f"    ОШИБКА: Столбец {column_index} не является словарем: {type(column_data)}", 'error')
                    continue
                
                # Определяем, является ли колонка премиумной
                is_premium = self.is_premium_column(column_data)
                
                if is_premium:
                    self.logger.log(f"    Столбец {column_index} определен как ПРЕМИУМНЫЙ (порог: {Constants.PREMIUM_THRESHOLD*100}%)")
                    # Для премиумных колонок используем отдельный счетчик
                    premium_column_index = premium_column_count
                    premium_column_count += 1
                else:
                    premium_column_index = column_index
                
                column_results = self.process_range_column(
                    column_data, country, vehicle_type_name, premium_column_index, is_premium
                )
                results.extend(column_results)
                self.logger.log(f"    Столбец {column_index}: обработано {len(column_results)} элементов")
        
        return results

    def parse_shop_data(self, shop_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Основной метод парсинга данных shop.blkx"""
        # Сначала очищаем от аномальных элементов
        cleaned_shop_data = self.clean_anomalous_elements(shop_data)
        
        # Извлекаем все поля image из очищенных данных
        self.extract_all_image_fields(cleaned_shop_data)
        
        # Затем собираем все master-slave пары на очищенных данных
        self.collect_master_slave_pairs(cleaned_shop_data)
        
        all_results = []
        
        for country_key, country_data in cleaned_shop_data.items():
            if country_key not in Constants.COUNTRY_MAPPING:
                continue
                
            country_name = Constants.COUNTRY_MAPPING[country_key]
            self.logger.log(f"Обработка страны: {country_name}")
            
            country_results = self.process_country_data(country_data, country_name)
            all_results.extend(country_results)
            
            self.logger.log(f"Обработано {len(country_results)} элементов для {country_name}")
        
        return all_results
    
    def save_image_fields_to_csv(self, filename: str = 'shop_images_fields.csv'):
        """Сохраняет извлеченные поля image в отдельный CSV файл"""
        if not self.image_fields:
            self.logger.log("Нет данных image полей для сохранения", 'warning')
            return
            
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['id', 'image_field']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for unit_id, image_field in self.image_fields.items():
                    writer.writerow({
                        'id': unit_id,
                        'image_field': image_field
                    })
                    
            self.logger.log(f"Поля image ({len(self.image_fields)} записей) сохранены в {filename}")
            
        except Exception as e:
            self.logger.log(f"Ошибка при сохранении полей image в CSV: {e}", 'error')
            raise
    
    def save_to_csv(self, data: List[Dict[str, Any]], filename: str = 'shop.csv'):
        """Сохраняет данные в CSV файл"""
        if not data:
            self.logger.log("Нет данных для сохранения", 'warning')
            return
            
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=Constants.CSV_FIELDNAMES, extrasaction='ignore')
                writer.writeheader()
                
                for item in data:
                    # Подготавливаем данные для записи
                    row_data = {
                        'id': item['id'].lower(),  # Приводим ID к нижнему регистру
                        'rank': item['rank'],
                        'country': item['country'],
                        'vehicle_type': item['vehicle_type'],
                        'type': item['type'],
                        'status': item['status'],
                        'column_index': item['column_index'],
                        'row_index': item['row_index'],
                        'predecessor': item['predecessor'].lower() if item['predecessor'] else '',  # Предшественник тоже ID
                        'order_in_folder': item.get('order_in_folder', '')
                    }
                    writer.writerow(row_data)
                    
            self.logger.log(f"Данные ({len(data)} записей) сохранены в {filename}")
            self.logger.log("Все ID приведены к нижнему регистру для совместимости")
            
            # Дополнительная статистика
            premium_count = sum(1 for item in data if item['status'] == 'premium')
            standard_count = len(data) - premium_count
            self.logger.log(f"Статистика: Стандартной техники: {standard_count}, Премиумной: {premium_count}")
            
            # Статистика по master-slave парам
            if self.master_slave_pairs:
                self.logger.log(f"Обработано master-slave пар: {len(self.master_slave_pairs)}")
                if not Constants.PROCESS_SLAVE_UNITS:
                    self.logger.log(f"Slave-юниты пропущены: {len(self.slave_units)}")
            
            # Сохраняем поля image в отдельный файл
            self.save_image_fields_to_csv()
            
        except Exception as e:
            self.logger.log(f"Ошибка при сохранении в CSV: {e}", 'error')
            raise

    def run(self):
        """Основной метод запуска парсера"""
        try:
            self.logger.log("Запуск парсера shop.blkx...")
            self.logger.log(f"Порог для определения премиумных колонок: {Constants.PREMIUM_THRESHOLD*100}%")
            self.logger.log(f"Обработка slave-юнитов: {'включена' if Constants.PROCESS_SLAVE_UNITS else 'отключена'}")
            self.logger.log(f"Аномальные окончания: {Constants.ANOMALOUS_SUFFIXES}")
            self.logger.log(f"Аномальные начала: {Constants.ANOMALOUS_PREFIXES}")
            
            # Загружаем данные
            shop_data = self.fetch_shop_data()
            
            # Парсим данные
            parsed_data = self.parse_shop_data(shop_data)
            
            # Сохраняем в CSV
            self.save_to_csv(parsed_data)
            
            self.logger.log(f"Парсинг завершен успешно! Обработано {len(parsed_data)} элементов.")
            
        except Exception as e:
            self.logger.log(f"Ошибка при выполнении парсинга: {e}", 'error')
            raise