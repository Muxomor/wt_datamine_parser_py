import json
import csv
import requests
from typing import Dict, List, Any, Optional, Set, Tuple

from collections import defaultdict
from utils import Config, Logger, Constants

HELICOPTERS_TYPE = Constants.VEHICLE_TYPE_MAPPING['helicopters']


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
                # Приводим к нижнему регистру: файлы на CDN всегда в lowercase
                # (например, datamine указывает germ_pzkpfw_III_group, а на CDN germ_pzkpfw_iii_group.png)
                formatted_image = image_path.split('#')[-1].lower()
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

    def has_premium_flag(self, item_data: Dict[str, Any]) -> bool:
        """Проверяет наличие явных премиумных флагов у юнита"""
        return any(indicator in item_data for indicator in Constants.PREMIUM_INDICATORS)

    def is_premium_vehicle(self, item_data: Dict[str, Any]) -> bool:
        """Определяет является ли техника премиумной (для определения премиумных колонок)"""
        return self.has_premium_flag(item_data)

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
        # 1. Основной признак - окончание на _group
        if item_name.endswith('_group'):
            return True
        
        # 2. Если есть slaveUnit - это НЕ группа, а master-unit
        if 'slaveUnit' in item_data:
            return False
            
        # 3. Проверяем наличие вложенных ЮНИТОВ (не служебных полей)
        nested_vehicle_items = []
        for key, value in item_data.items():
            if (key not in Constants.SERVICE_FIELDS and 
                isinstance(value, dict) and 
                self._looks_like_vehicle_data(value)):
                nested_vehicle_items.append(key)
        
        return len(nested_vehicle_items) > 0

    def _looks_like_vehicle_data(self, data: Dict[str, Any]) -> bool:
        """Проверяет, выглядят ли данные как информация о технике"""
        if not isinstance(data, dict):
            return False
            
        # Юнит должен иметь ранг или быть простым словарем без служебных полей
        return ('rank' in data or 
                (isinstance(data, dict) and 
                 not any(key.startswith('#') for key in data.keys()) and
                 len(data) > 0))

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

    def _get_rank_pos_xy(self, item_info: Dict[str, Any]) -> Optional[List[int]]:
        """Извлекает rankPosXY [x, y] из данных юнита (1-based координаты из shop.blkx)."""
        raw = item_info.get('rankPosXY')
        if not raw or not isinstance(raw, (list, tuple)) or len(raw) < 2:
            return None
        return [int(raw[0]), int(raw[1])]

    def _is_helicopters(self, vehicle_type: str) -> bool:
        return vehicle_type == HELICOPTERS_TYPE

    def _is_helicopter_premium(self, item_info: Dict[str, Any], rank_pos_xy: Optional[List[int]]) -> bool:
        """Premium-секция вертолётов: rankPosXY.x >= 6 или явные premium-флаги."""
        if rank_pos_xy and rank_pos_xy[0] >= Constants.HELI_PREMIUM_X_MIN:
            return True
        return self.has_premium_flag(item_info)

    def _resolve_predecessor(
        self,
        item_info: Dict[str, Any],
        is_premium: bool,
        results: List[Dict[str, Any]],
        next_should_depend_on_group: Optional[str] = None,
        is_helicopter: bool = False,
    ) -> str:
        """Определяет predecessor: для вертолётов reqAir имеет приоритет над sequential."""
        if is_premium:
            return ''

        req_air = item_info.get('reqAir')
        if req_air == '':
            return ''

        if is_helicopter and req_air:
            return req_air

        if next_should_depend_on_group:
            return next_should_depend_on_group
        if results:
            return results[-1]['id']
        return ''

    def assign_helicopter_coordinates(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Назначает координаты вертолётам из rankPosXY с dense-rank remap X по дереву нации."""
        self.logger.log("Назначение координат вертолётов из rankPosXY...")

        heli_by_country: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in parsed_data:
            if self._is_helicopters(item['vehicle_type']):
                heli_by_country[item['country']].append(item)

        for country, items in heli_by_country.items():
            standard_xs = sorted({
                item['rank_pos_xy'][0]
                for item in items
                if item.get('rank_pos_xy') and item['status'] == 'standard'
            })
            premium_xs = sorted({
                item['rank_pos_xy'][0]
                for item in items
                if item.get('rank_pos_xy') and item['status'] == 'premium'
            })
            standard_x_map = {x: i for i, x in enumerate(standard_xs)}
            premium_x_map = {x: i for i, x in enumerate(premium_xs)}

            self.logger.log(
                f"  {country}: standard X {standard_xs} → cols 0..{len(standard_xs)-1}, "
                f"premium X {premium_xs} → cols 0..{len(premium_xs)-1}"
            )

            for item in items:
                xy = item.get('rank_pos_xy')
                if not xy:
                    self.logger.log(
                        f"    ПРЕДУПРЕЖДЕНИЕ: {item['id']} без rankPosXY, координаты не назначены",
                        'warning',
                    )
                    continue

                x, y = xy[0], xy[1]
                if item['status'] == 'premium':
                    if x not in premium_x_map:
                        self.logger.log(
                            f"    ПРЕДУПРЕЖДЕНИЕ: premium {item['id']} X={x} вне premium_x_map",
                            'warning',
                        )
                        continue
                    item['column_index'] = premium_x_map[x]
                else:
                    if x not in standard_x_map:
                        self.logger.log(
                            f"    ПРЕДУПРЕЖДЕНИЕ: {item['id']} X={x} вне standard_x_map",
                            'warning',
                        )
                        continue
                    item['column_index'] = standard_x_map[x]

                item['row_index'] = y - 1
                self.logger.log(
                    f"    {item['id']}: rankPosXY [{x}, {y}] → [{item['column_index']}, {item['row_index']}] "
                    f"({item['status']})",
                    'debug',
                )

        return parsed_data

    def normalize_folder_predecessors(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Перенаправляет predecessor на папку, если внешний юнит зависит от элемента внутри folder.

        В shop.blkx reqAir может указывать на конкретный вариант в группе (tiger_had_france),
        но на сетке отображается только folder (tiger_group). Внутри папки цепочки сохраняются.
        """
        by_id = {item['id']: item for item in parsed_data}

        for item in parsed_data:
            pred_id = item.get('predecessor')
            if not pred_id:
                continue

            pred_item = by_id.get(pred_id)
            if not pred_item:
                continue

            folder_id = pred_item.get('parent_id')
            if not folder_id:
                continue

            if item.get('parent_id') == folder_id:
                continue

            item['predecessor'] = folder_id
            self.logger.log(
                f"  normalize_folder_predecessors: {item['id']} -> {folder_id} "
                f"(было {pred_id}, элемент внутри folder)",
                'debug',
            )

        return parsed_data

    def assign_coordinates_after_parsing(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Назначает координаты после парсинга всех данных на основе структурного анализа"""
        self.logger.log("Начинаем назначение координат...")

        parsed_data = self.assign_helicopter_coordinates(parsed_data)
        
        # Группируем данные по (страна, тип_техники, original_column_pos, is_premium)
        columns_structure = {}
        
        for item in parsed_data:
            if self._is_helicopters(item['vehicle_type']):
                continue

            key = (
                item['country'], 
                item['vehicle_type'], 
                item['original_column_pos'], 
                item['status'] == 'premium'
            )
            
            if key not in columns_structure:
                columns_structure[key] = []
            columns_structure[key].append(item)
        
        # Для каждой группы (страна+тип+столбец) назначаем координаты
        premium_column_counters = {}  # (страна, тип) -> счетчик премиум столбцов
        
        for (country, vehicle_type, original_column_pos, is_premium), items in columns_structure.items():
            self.logger.log(f"  Обработка столбца: {country}/{vehicle_type}/col_{original_column_pos}/premium_{is_premium}")
            
            # Определяем финальный column_index
            if is_premium:
                # Для премиумной техники используем отдельный счетчик
                key = (country, vehicle_type)
                if key not in premium_column_counters:
                    premium_column_counters[key] = 0
                final_column_index = premium_column_counters[key]
                premium_column_counters[key] += 1
            else:
                # Для обычной техники используем исходную позицию
                final_column_index = original_column_pos
            
            # Сохраняем исходный порядок элементов и группируем по рангу
            items_by_rank = {}
            for item in items:
                rank = item['rank']
                if rank not in items_by_rank:
                    items_by_rank[rank] = []
                items_by_rank[rank].append(item)
            
            # Назначаем row_index: сброс при смене ранга
            current_row = 0
            for rank in sorted(items_by_rank.keys()):
                rank_items = items_by_rank[rank]
                
                self.logger.log(f"    Ранг {rank}: {len(rank_items)} элементов, начинаем с row_index={current_row}")
                
                # Сортируем элементы ранга в том порядке, в котором они шли в исходных данных
                # Используем поле parsing_order которое мы добавим
                rank_items.sort(key=lambda x: x.get('parsing_order', 0))
                
                # Группируем элементы ранга по логическим группам, сохраняя порядок
                logical_groups = self._group_items_by_logical_groups_preserve_order(rank_items)
                
                # Назначаем координаты для каждой логической группы
                for group_items in logical_groups:
                    # Все элементы логической группы получают одинаковые координаты
                    for item in group_items:
                        item['column_index'] = final_column_index
                        item['row_index'] = current_row
                        self.logger.log(f"      {item['id']}: [{final_column_index}, {current_row}] ({'группа' if item.get('is_group') else 'элемент'})", 'debug')
                    
                    # Переходим к следующему row только после обработки всей логической группы
                    current_row += 1
                
                # После обработки ранга сбрасываем row для следующего ранга
                current_row = 0
        
        self.logger.log(f"Координаты назначены для {len(parsed_data)} элементов")
        return parsed_data

    def _group_items_by_logical_groups_preserve_order(self, rank_items: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Группирует элементы ранга по логическим группам, СОХРАНЯЯ исходный порядок"""
        logical_groups = []
        processed_items = set()
        
        # Проходим по элементам в том порядке, в котором они шли в исходных данных
        for item in rank_items:
            if item['id'] in processed_items:
                continue
                
            if item.get('is_group', False):
                # Это группа - собираем её со всеми элементами
                group_id = item['id']
                logical_group = [item]  # Начинаем с самой группы
                processed_items.add(item['id'])
                
                # Ищем все элементы этой группы в том же порядке
                for child_item in rank_items:
                    if (child_item.get('parent_id') == group_id and 
                        child_item['id'] not in processed_items):
                        logical_group.append(child_item)
                        processed_items.add(child_item['id'])
                
                logical_groups.append(logical_group)
                self.logger.log(f"        Логическая группа: {group_id} + {len(logical_group)-1} элементов", 'debug')
                
            elif not item.get('parent_id'):
                # Это отдельный элемент (не принадлежит группе)
                logical_groups.append([item])
                processed_items.add(item['id'])
                self.logger.log(f"        Отдельный элемент: {item['id']}", 'debug')
            
            # Элементы групп пропускаем - они уже обработаны выше
        
        return logical_groups

    def process_range_column(self, range_data: Dict[str, Any], country: str, 
                           vehicle_type: str, original_column_pos: int, is_premium: bool = False) -> List[Dict[str, Any]]:
        """Обрабатывает один столбец (range) техники - ТОЛЬКО сбор данных, БЕЗ координат"""
        results = []
        next_should_depend_on_group = None
        parsing_order = 0  # Счетчик для сохранения порядка парсинга
        is_helicopter = self._is_helicopters(vehicle_type)
        
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
                group_results = self._process_group(item_name, item_info, country, vehicle_type, 
                                                 original_column_pos, is_premium, results, 
                                                 next_should_depend_on_group, parsing_order,
                                                 is_helicopter=is_helicopter)
                
                # Устанавливаем parsing_order для всех элементов группы
                for group_item in group_results:
                    group_item['parsing_order'] = parsing_order
                
                results.extend(group_results)
                parsing_order += 1
                
                # После группы следующий элемент должен зависеть от группы
                last_group_item = group_results[-1] if group_results else None
                if (last_group_item and last_group_item.get('status') != 'premium'
                        and not (item_name in self.slave_units and not Constants.PROCESS_SLAVE_UNITS)):
                    next_should_depend_on_group = item_name
                    self.logger.log(f"        ОТЛАДКА: Установлен флаг next_should_depend_on_group={item_name}", 'debug')
                    
            else:
                # Обрабатываем обычную технику
                regular_item = self._process_regular_item(item_name, item_info, country, vehicle_type,
                                                        original_column_pos, is_premium, results,
                                                        next_should_depend_on_group,
                                                        is_helicopter=is_helicopter)
                if regular_item:
                    regular_item['parsing_order'] = parsing_order
                    results.append(regular_item)
                    parsing_order += 1
                    next_should_depend_on_group = None
        
        return results

    def _process_group(self, item_name: str, item_info: Dict[str, Any], country: str, 
                      vehicle_type: str, original_column_pos: int, is_premium: bool,
                      results: List[Dict[str, Any]], next_should_depend_on_group: Optional[str], 
                      parsing_order: int, is_helicopter: bool = False) -> List[Dict[str, Any]]:
        """Обрабатывает группу техники"""
        group_results = []
        rank = item_info.get('rank', 1)
        group_rank_pos_xy = self._get_rank_pos_xy(item_info)
        
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
                                               original_column_pos, is_premium, actual_rank,
                                               results, next_should_depend_on_group,
                                               is_helicopter=is_helicopter,
                                               inherited_rank_pos_xy=group_rank_pos_xy)
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
                                                      vehicle_type, original_column_pos, is_premium,
                                                      actual_rank, order, group_items,
                                                      is_helicopter=is_helicopter,
                                                      inherited_rank_pos_xy=group_rank_pos_xy)
            if nested_item:
                group_results.append(nested_item)
        
        return group_results

    def _create_group_item(self, item_name: str, item_info: Dict[str, Any], country: str,
                          vehicle_type: str, original_column_pos: int, is_premium: bool,
                          actual_rank: int, results: List[Dict[str, Any]], 
                          next_should_depend_on_group: Optional[str],
                          is_helicopter: bool = False,
                          inherited_rank_pos_xy: Optional[List[int]] = None) -> Optional[Dict[str, Any]]:
        """Создает элемент группы - БЕЗ назначения координат"""
        rank_pos_xy = inherited_rank_pos_xy or self._get_rank_pos_xy(item_info)
        item_is_premium = (
            self._is_helicopter_premium(item_info, rank_pos_xy)
            if is_helicopter
            else is_premium
        )

        predecessor = self._resolve_predecessor(
            item_info, item_is_premium, results, next_should_depend_on_group, is_helicopter
        )
        self.logger.log(
            f"        ОТЛАДКА: Группа {item_name}, predecessor='{predecessor}', "
            f"rankPosXY={rank_pos_xy}, premium={item_is_premium}",
            'debug',
        )
        
        group_item = {
            'id': item_name,
            'rank': actual_rank,
            'country': country,
            'vehicle_type': vehicle_type,
            'type': 'folder',
            'status': 'premium' if item_is_premium else 'standard',
            'reqAir': item_info.get('reqAir', None),
            'is_group': True,
            'order_in_folder': None,
            'predecessor': predecessor,
            'original_column_pos': original_column_pos,
            'have_prem_flag': self.has_premium_flag(item_info),
            'rank_pos_xy': rank_pos_xy,
            'column_index': 0,
            'row_index': 0
        }
        
        self.logger.log(f"        ОТЛАДКА: Добавлена группа {item_name} с предшественником '{predecessor}', статус: {group_item['status']}", 'debug')
        return group_item

    def _create_group_child_item(self, nested_name: str, nested_info: Dict[str, Any], parent_name: str,
                               country: str, vehicle_type: str, original_column_pos: int, is_premium: bool,
                               actual_rank: int, order: int, group_items: List[tuple],
                               is_helicopter: bool = False,
                               inherited_rank_pos_xy: Optional[List[int]] = None) -> Optional[Dict[str, Any]]:
        """Создает элемент внутри группы - БЕЗ назначения координат"""
        nested_rank = nested_info.get('rank', actual_rank)
        rank_pos_xy = self._get_rank_pos_xy(nested_info) or inherited_rank_pos_xy
        
        # Для slave-unit берем только основной элемент
        parent_id = self.master_slave_pairs.get(parent_name, parent_name)
        
        item_is_premium = (
            self._is_helicopter_premium(nested_info, rank_pos_xy)
            if is_helicopter
            else is_premium
        )

        predecessor = ''
        if not item_is_premium:
            req_air = nested_info.get('reqAir')
            if is_helicopter and req_air:
                predecessor = req_air
            elif req_air == '':
                predecessor = ''
            elif order == 0:
                predecessor = parent_id
                self.logger.log(f"          ОТЛАДКА: Первый элемент группы {nested_name} зависит от группы {parent_id}", 'debug')
            else:
                prev_order = order - 1
                for prev_order_item, prev_name, prev_info in group_items:
                    if prev_order_item == prev_order:
                        if not Constants.PROCESS_SLAVE_UNITS and prev_name in self.slave_units:
                            continue
                        predecessor = prev_name
                        self.logger.log(f"          ОТЛАДКА: Элемент группы {nested_name} зависит от {prev_name}", 'debug')
                        break
        
        nested_item = {
            'id': nested_name,
            'rank': nested_rank,
            'country': country,
            'vehicle_type': vehicle_type,
            'type': 'vehicle',
            'status': 'premium' if item_is_premium else 'standard',
            'reqAir': nested_info.get('reqAir', None),
            'is_group': False,
            'parent_id': parent_id,
            'order_in_folder': order,
            'predecessor': predecessor,
            'original_column_pos': original_column_pos,
            'have_prem_flag': self.has_premium_flag(nested_info),
            'rank_pos_xy': rank_pos_xy,
            'column_index': 0,
            'row_index': 0
        }
        
        self.logger.log(f"          ОТЛАДКА: Добавлен элемент группы {nested_name} с предшественником '{predecessor}'", 'debug')
        return nested_item

    def _process_regular_item(self, item_name: str, item_info: Dict[str, Any], country: str,
                            vehicle_type: str, original_column_pos: int, is_premium: bool,
                            results: List[Dict[str, Any]], next_should_depend_on_group: Optional[str],
                            is_helicopter: bool = False) -> Optional[Dict[str, Any]]:
        """Обрабатывает обычную технику - БЕЗ назначения координат"""
        rank = item_info.get('rank', 1)
        rank_pos_xy = self._get_rank_pos_xy(item_info)
        item_is_premium = (
            self._is_helicopter_premium(item_info, rank_pos_xy)
            if is_helicopter
            else is_premium
        )

        predecessor = self._resolve_predecessor(
            item_info, item_is_premium, results, next_should_depend_on_group, is_helicopter
        )
        self.logger.log(
            f"      ОТЛАДКА: Техника {item_name}, predecessor='{predecessor}', "
            f"rankPosXY={rank_pos_xy}, premium={item_is_premium}",
            'debug',
        )
            
        regular_item = {
            'id': item_name,
            'rank': rank,
            'country': country,
            'vehicle_type': vehicle_type,
            'type': 'vehicle',
            'status': 'premium' if item_is_premium else 'standard',
            'reqAir': item_info.get('reqAir', None),
            'is_group': False,
            'order_in_folder': None,
            'predecessor': predecessor,
            'original_column_pos': original_column_pos,
            'have_prem_flag': self.has_premium_flag(item_info),
            'rank_pos_xy': rank_pos_xy,
            'column_index': 0,
            'row_index': 0
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
            is_helicopter = self._is_helicopters(vehicle_type_name)
            
            # Обрабатываем каждый столбец (range)
            for column_index, column_data in enumerate(range_data):
                if not isinstance(column_data, dict):
                    self.logger.log(f"    ОШИБКА: Столбец {column_index} не является словарем: {type(column_data)}", 'error')
                    continue
                
                # Для вертолётов premium определяется по rankPosXY.x >= 6 на уровне юнита
                is_premium = False if is_helicopter else self.is_premium_column(column_data)
                
                if is_premium:
                    self.logger.log(f"    Столбец {column_index} определен как ПРЕМИУМНЫЙ (порог: {Constants.PREMIUM_THRESHOLD*100}%)")
                elif not is_helicopter:
                    self.logger.log(f"    Столбец {column_index} определен как ОБЫЧНЫЙ")
                else:
                    self.logger.log(f"    Столбец {column_index}: вертолёты — premium по rankPosXY.x")
                
                column_results = self.process_range_column(
                    column_data, country, vehicle_type_name, column_index, is_premium
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
        
        all_results = self.assign_coordinates_after_parsing(all_results)
        all_results = self.normalize_folder_predecessors(all_results)

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
                        'order_in_folder': item.get('order_in_folder', ''),
                        'have_prem_flag': item.get('have_prem_flag', False)  # НОВОЕ ПОЛЕ
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