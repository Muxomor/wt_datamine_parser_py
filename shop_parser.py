def process_range_column(self, range_data: Dict[str, Any], country: str,import json
import csv
import requests
from typing import Dict, List, Any, Optional
import sys
import os

class ShopParser:
    def __init__(self, config_path: str = 'config.txt'):
        """
        Инициализация парсера shop.blkx
        
        Args:
            config_path: Путь к конфигурационному файлу
        """
        self.config = self.read_config(config_path)
        self.vehicles_data = []
        
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

    def read_config(self, config_path: str) -> Dict[str, str]:
        """Читает конфигурационный файл"""
        config = {}
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if '=' not in line:
                            print(f"Предупреждение: Пропускаем некорректную строку #{line_num}: {line}")
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
            print(f"Загрузка данных из: {shop_url}")
            response = requests.get(shop_url, timeout=30)
            response.raise_for_status()
            
            shop_data = response.json()
            print(f"Данные успешно загружены")
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
                items.append((order, item_name, item_data))
                
        return items

    def process_range_column(self, range_data: Dict[str, Any], country: str, 
                           vehicle_type: str, column_index: int) -> List[Dict[str, Any]]:
        """Обрабатывает один столбец (range) техники"""
        results = []
        
        # Обрабатываем элементы в том порядке, в котором они идут в JSON
        for item_name, item_info in range_data.items():
            if not isinstance(item_info, dict):
                print(f"      ПРЕДУПРЕЖДЕНИЕ: Элемент {item_name} не является словарем: {type(item_info)}")
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
                    actual_rank = first_item_info.get('rank', rank)
                else:
                    actual_rank = rank
                
                # Добавляем саму группу (если не slave-unit)
                group_item = None
                if not self.is_slave_unit(item_info):
                    # Получаем координаты из rankPosXY если есть
                    pos_xy = item_info.get('rankPosXY', [column_index, actual_rank - 1])
                    
                    # Определяем предшественника для группы
                    predecessor = ''
                    if item_info.get('reqAir') != '':
                        # Группа зависит от предыдущего элемента в столбце
                        if results:
                            predecessor = results[-1]['id']
                    
                    group_item = {
                        'id': item_name,
                        'rank': actual_rank,
                        'country': country,
                        'vehicle_type': vehicle_type,
                        'type': 'folder',
                        'reqAir': item_info.get('reqAir', ''),
                        'is_group': True,
                        'order_in_folder': None,
                        'predecessor': predecessor,
                        'column_index': pos_xy[0] if 'rankPosXY' in item_info else column_index,
                        'row_index': pos_xy[1] if 'rankPosXY' in item_info else actual_rank - 1
                    }
                    
                    results.append(group_item)
                
                # Обрабатываем элементы внутри группы
                for order, nested_name, nested_info in group_items:
                    if not isinstance(nested_info, dict):
                        print(f"        ПРЕДУПРЕЖДЕНИЕ: Элемент группы {nested_name} не является словарем: {type(nested_info)}")
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
                    if nested_info.get('reqAir') != '':
                        if order == 0:
                            # Первый элемент группы зависит от самой группы
                            predecessor = parent_id
                        else:
                            # Остальные элементы зависят от предыдущего элемента группы
                            prev_order = order - 1
                            for prev_order_item, prev_name, prev_info in group_items:
                                if prev_order_item == prev_order:
                                    predecessor = prev_name
                                    break
                        
                    nested_item = {
                        'id': nested_name,
                        'rank': nested_rank,
                        'country': country,
                        'vehicle_type': vehicle_type,
                        'type': 'vehicle',
                        'reqAir': nested_info.get('reqAir', ''),
                        'is_group': False,
                        'parent_id': parent_id,
                        'order_in_folder': order,
                        'predecessor': predecessor,
                        'column_index': pos_xy[0] if ('rankPosXY' in nested_info or 'rankPosXY' in item_info) else column_index,
                        'row_index': pos_xy[1] if ('rankPosXY' in nested_info or 'rankPosXY' in item_info) else nested_rank - 1
                    }
                    
                    results.append(nested_item)
                
                # После группы следующий элемент должен зависеть от группы
                # Устанавливаем специальный флаг для следующего элемента
                if group_item and not self.is_slave_unit(item_info):
                    # Запоминаем ID группы для следующего элемента
                    next_should_depend_on_group = item_name
                else:
                    next_should_depend_on_group = None
                    
            else:
                # Обычная техника
                rank = item_info.get('rank', 1)
                
                # Получаем координаты из rankPosXY если есть
                pos_xy = item_info.get('rankPosXY', [column_index, rank - 1])
                
                # Определяем предшественника
                predecessor = ''
                if item_info.get('reqAir') != '':
                    # Проверяем, должен ли этот элемент зависеть от группы
                    if 'next_should_depend_on_group' in locals() and next_should_depend_on_group:
                        predecessor = next_should_depend_on_group
                        next_should_depend_on_group = None  # Сбрасываем флаг
                    elif results:
                        # Обычная зависимость от предыдущего элемента
                        predecessor = results[-1]['id']
                    
                regular_item = {
                    'id': item_name,
                    'rank': rank,
                    'country': country,
                    'vehicle_type': vehicle_type,
                    'type': 'vehicle',
                    'reqAir': item_info.get('reqAir', ''),
                    'is_group': False,
                    'order_in_folder': None,
                    'predecessor': predecessor,
                    'column_index': pos_xy[0] if 'rankPosXY' in item_info else column_index,
                    'row_index': pos_xy[1] if 'rankPosXY' in item_info else rank - 1
                }
                
                results.append(regular_item)
        
        return results

    def determine_predecessor(self, current_item: Dict[str, Any], rank_items: List[Dict[str, Any]], 
                            current_index: int, all_results: List[Dict[str, Any]]) -> str:
        """Определяет предшественника для текущего элемента"""
        
        # Если есть reqAir = "", предшественника нет
        if current_item.get('reqAir') == '':
            return ''
        
        # Если элемент в группе
        if current_item.get('parent_id'):
            parent_id = current_item['parent_id']
            
            # Ищем все элементы в той же группе в том же ранге
            same_group_items = []
            for item in rank_items:
                if item.get('parent_id') == parent_id:
                    same_group_items.append(item)
            
            # Сортируем по порядку в группе
            same_group_items.sort(key=lambda x: x.get('order_in_folder', 0))
            
            current_order = current_item.get('order_in_folder', 0)
            
            # Ищем предыдущий элемент в группе
            for item in same_group_items:
                if item.get('order_in_folder', 0) == current_order - 1:
                    return item['id']
                    
            # Если это первый элемент в группе, предшественник - сама группа
            return parent_id
        
        # Для обычной техники и групп
        current_rank = current_item['rank']
        current_column = current_item['column_index']
        
        # Сначала ищем предшественника в том же ранге (предыдущий элемент)
        same_rank_items = [item for item in rank_items if item['rank'] == current_rank and item['column_index'] == current_column]
        same_rank_items.sort(key=lambda x: rank_items.index(x))  # Сортируем по порядку в списке
        
        current_position = None
        for i, item in enumerate(same_rank_items):
            if item['id'] == current_item['id']:
                current_position = i
                break
                
        if current_position is not None and current_position > 0:
            return same_rank_items[current_position - 1]['id']
        
        # Если это первый элемент в ранге, ищем последний элемент предыдущего ранга в том же столбце
        for item in reversed(all_results):
            if (item['rank'] == current_rank - 1 and 
                item['column_index'] == current_column):
                return item['id']
                
        return ''

    def process_country_data(self, country_data: Dict[str, Any], country: str) -> List[Dict[str, Any]]:
        """Обрабатывает данные одной страны"""
        results = []
        
        for vehicle_type, type_data in country_data.items():
            if vehicle_type not in self.vehicle_type_mapping:
                continue
                
            vehicle_type_name = self.vehicle_type_mapping[vehicle_type]
            range_data = type_data.get('range', [])
            
            print(f"  Обработка типа: {vehicle_type_name}, столбцов: {len(range_data)}")
            
            # Обрабатываем каждый столбец (range)
            for column_index, column_data in enumerate(range_data):
                if not isinstance(column_data, dict):
                    print(f"    ОШИБКА: Столбец {column_index} не является словарем: {type(column_data)}")
                    continue
                    
                column_results = self.process_range_column(
                    column_data, country, vehicle_type_name, column_index
                )
                results.extend(column_results)
                print(f"    Столбец {column_index}: обработано {len(column_results)} элементов")
        
        return results

    def parse_shop_data(self, shop_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Основной метод парсинга данных shop.blkx"""
        all_results = []
        
        for country_key, country_data in shop_data.items():
            if country_key not in self.country_mapping:
                continue
                
            country_name = self.country_mapping[country_key]
            print(f"Обработка страны: {country_name}")
            
            country_results = self.process_country_data(country_data, country_name)
            all_results.extend(country_results)
            
            print(f"Обработано {len(country_results)} элементов для {country_name}")
        
        return all_results

    def save_to_csv(self, data: List[Dict[str, Any]], filename: str = 'shop.csv'):
        """Сохраняет данные в CSV файл"""
        if not data:
            print("Нет данных для сохранения")
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
                    
            print(f"Данные ({len(data)} записей) сохранены в {filename}")
            
        except Exception as e:
            print(f"Ошибка при сохранении в CSV: {e}")
            raise

    def run(self):
        """Основной метод запуска парсера"""
        try:
            print("Запуск парсера shop.blkx...")
            
            # Загружаем данные
            shop_data = self.fetch_shop_data()
            
            # Парсим данные
            parsed_data = self.parse_shop_data(shop_data)
            
            # Сохраняем в CSV
            self.save_to_csv(parsed_data)
            
            print(f"Парсинг завершен успешно! Обработано {len(parsed_data)} элементов.")
            
        except Exception as e:
            print(f"Ошибка при выполнении парсинга: {e}")
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