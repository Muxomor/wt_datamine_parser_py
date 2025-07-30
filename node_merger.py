import csv
import os
from typing import Dict, List, Any, Optional, Set
from utils import Logger


class ModernNodesMerger:
    """
    Современный класс для объединения данных из различных CSV файлов проекта War Thunder
    и создания файла зависимостей между техникой.
    """
    
    def __init__(self, config_path: str = 'config.txt'):
        """
        Инициализация ModernNodesMerger
        
        Args:
            config_path: Путь к конфигурационному файлу
        """
        self.logger = Logger('nodes_merger', 'nodes_merger_debug.log')
        self.merged_data: List[Dict[str, Any]] = []
        
        # Словари для быстрого поиска
        self.localization_dict: Dict[str, str] = {}
        self.wpcost_dict: Dict[str, Dict[str, Any]] = {}
        self.images_dict: Dict[str, str] = {}
        
    def load_csv_data(self, filepath: str) -> List[Dict[str, Any]]:
        """Загружает данные из CSV файла"""
        if not os.path.exists(filepath):
            self.logger.log(f"Файл {filepath} не найден, пропускаем", 'warning')
            return []
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data = list(reader)
                self.logger.log(f"Загружено {len(data)} записей из {filepath}")
                return data
        except Exception as e:
            self.logger.log(f"Ошибка при загрузке {filepath}: {e}", 'error')
            return []
    
    def load_supporting_data(self, localization_file: str = 'localization.csv',
                           wpcost_file: str = 'wpcost.csv', 
                           images_file: str = 'shop_images.csv'):
        """Загружает вспомогательные данные для обогащения основных данных"""
        self.logger.log("Загрузка вспомогательных данных...")
        
        # Загрузка локализации
        localization_data = self.load_csv_data(localization_file)
        for row in localization_data:
            unit_id = row.get('id', '').strip().lower()
            localized_name = row.get('localized_name', '').strip()
            if unit_id and localized_name:
                self.localization_dict[unit_id] = localized_name
        
        self.logger.log(f"Загружено локализаций: {len(self.localization_dict)}")
        
        # Загрузка экономических данных
        wpcost_data = self.load_csv_data(wpcost_file)
        for row in wpcost_data:
            unit_id = row.get('id', '').strip().lower()
            if unit_id:
                self.wpcost_dict[unit_id] = {
                    'silver': self._safe_int(row.get('silver')),
                    'exp': self._safe_int(row.get('exp')),
                    'br': self._safe_float(row.get('br'))
                }
        
        self.logger.log(f"Загружено экономических данных: {len(self.wpcost_dict)}")
        
        # Загрузка изображений
        images_data = self.load_csv_data(images_file)
        for row in images_data:
            unit_id = row.get('id', '').strip().lower()
            image_url = row.get('image_url', '').strip()
            if unit_id and image_url:
                self.images_dict[unit_id] = image_url
        
        self.logger.log(f"Загружено изображений: {len(self.images_dict)}")
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """Безопасное преобразование в int"""
        if value is None or value == '':
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Безопасное преобразование в float"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def merge_shop_data(self, shop_file: str = 'shop.csv') -> List[Dict[str, Any]]:
        """
        Объединяет данные из shop.csv с вспомогательными данными
        
        Args:
            shop_file: Путь к файлу shop.csv
            
        Returns:
            Список объединенных записей
        """
        self.logger.log("Начало объединения данных...")
        
        # Загружаем основные данные
        shop_data = self.load_csv_data(shop_file)
        if not shop_data:
            self.logger.log("Нет данных из shop.csv для обработки", 'error')
            return []
        
        # Загружаем вспомогательные данные
        self.load_supporting_data()
        
        merged_data = []
        localization_found = 0
        wpcost_found = 0
        images_found = 0
        
        for row in shop_data:
            unit_id = row.get('id', '').strip().lower()
            if not unit_id:
                continue
            
            # Создаем объединенную запись только с нужными полями
            merged_record = {
                # Основные поля
                'external_id': unit_id,
                'name': '',  # Будет заполнено из локализации
                'country': row.get('country', '').strip(),
                'battle_rating': '',  # Будет заполнено из wpcost как строка
                'silver': None,  # Будет заполнено из wpcost
                'rank': self._safe_int(row.get('rank')),
                'vehicle_category': row.get('vehicle_type', '').strip(),
                'type': row.get('type', 'vehicle').strip(),
                'required_exp': None,  # Будет заполнено из wpcost
                'tech_category': row.get('status', 'standard').strip(),
                'image_url': '',  # Будет заполнено из images
                'parent_external_id': row.get('predecessor', '').strip().lower(),
                'column': self._safe_int(row.get('column_index')),
                'row': self._safe_int(row.get('row_index')),
                'order_in_folder': self._safe_int(row.get('order_in_folder'))
            }
            
            # Обогащаем локализацией
            if unit_id in self.localization_dict:
                merged_record['name'] = self.localization_dict[unit_id]
                localization_found += 1
            else:
                # Используем ID как fallback имя
                merged_record['name'] = unit_id
            
            # Обогащаем экономическими данными
            if unit_id in self.wpcost_dict:
                wpcost_info = self.wpcost_dict[unit_id]
                merged_record['silver'] = wpcost_info['silver']
                merged_record['required_exp'] = wpcost_info['exp']
                
                # Форматируем BR как строку
                if wpcost_info['br'] is not None:
                    merged_record['battle_rating'] = str(wpcost_info['br'])
                
                wpcost_found += 1
            
            # Обогащаем изображениями
            if unit_id in self.images_dict:
                merged_record['image_url'] = self.images_dict[unit_id]
                images_found += 1
            
            merged_data.append(merged_record)
        
        # Статистика обогащения
        total_records = len(merged_data)
        self.logger.log(f"Объединение завершено:")
        self.logger.log(f"  Всего записей: {total_records}")
        self.logger.log(f"  Найдено локализаций: {localization_found} ({localization_found/total_records*100:.1f}%)")
        self.logger.log(f"  Найдено экономических данных: {wpcost_found} ({wpcost_found/total_records*100:.1f}%)")
        self.logger.log(f"  Найдено изображений: {images_found} ({images_found/total_records*100:.1f}%)")
        
        self.merged_data = merged_data
        return merged_data
    
    def extract_dependencies(self, merged_data: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, str]]:
        """
        Извлекает зависимости между узлами на основе поля 'parent_external_id'
        
        Args:
            merged_data: Данные для извлечения зависимостей (если None, используются self.merged_data)
            
        Returns:
            Список зависимостей в формате [{'node_external_id': 'child', 'prerequisite_external_id': 'parent'}]
        """
        if merged_data is None:
            merged_data = self.merged_data
            
        if not merged_data:
            self.logger.log("Нет данных для извлечения зависимостей", 'warning')
            return []
        
        self.logger.log("Извлечение зависимостей...")
        
        # Создаем словарь всех узлов для проверки существования родителей
        nodes_by_id = {item['external_id']: item for item in merged_data}
        
        dependencies = []
        processed_nodes = 0
        dependencies_found = 0
        parent_not_found_count = 0
        empty_parent_count = 0
        
        for item in merged_data:
            processed_nodes += 1
            node_id = item['external_id']
            parent_id = item.get('parent_external_id', '').strip()
            
            if not parent_id:
                empty_parent_count += 1
                continue
            
            # Проверяем существование предшественника
            if parent_id in nodes_by_id:
                dependencies.append({
                    'node_external_id': node_id,
                    'prerequisite_external_id': parent_id
                })
                dependencies_found += 1
                self.logger.log(f"  Зависимость: {node_id} -> {parent_id}", 'debug')
            else:
                # Пробуем найти с суффиксом _group (для совместимости со старой логикой)
                alt_parent_id = parent_id + "_group"
                if alt_parent_id in nodes_by_id:
                    dependencies.append({
                        'node_external_id': node_id,
                        'prerequisite_external_id': alt_parent_id
                    })
                    dependencies_found += 1
                    self.logger.log(f"  Зависимость (alt): {node_id} -> {alt_parent_id}", 'debug')
                else:
                    parent_not_found_count += 1
                    self.logger.log(f"  Предшественник не найден: {node_id} -> {parent_id}", 'debug')
        
        # Статистика извлечения
        self.logger.log(f"Извлечение зависимостей завершено:")
        self.logger.log(f"  Обработано узлов: {processed_nodes}")
        self.logger.log(f"  Найдено зависимостей: {dependencies_found}")
        self.logger.log(f"  Пустых предшественников: {empty_parent_count}")
        self.logger.log(f"  Предшественников не найдено: {parent_not_found_count}")
        
        return dependencies
    
    def save_merged_data(self, merged_data: List[Dict[str, Any]], filename: str = 'vehicles_merged.csv'):
        """Сохраняет объединенные данные в CSV файл"""
        if not merged_data:
            self.logger.log("Нет данных для сохранения", 'warning')
            return
        
        # Определяем только нужные поля в правильном порядке
        fieldnames = [
            'external_id', 'name', 'country', 'battle_rating', 'silver', 'rank',
            'vehicle_category', 'type', 'required_exp', 'tech_category', 'image_url',
            'parent_external_id', 'column', 'row', 'order_in_folder'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                
                for item in merged_data:
                    writer.writerow(item)
            
            self.logger.log(f"Объединенные данные ({len(merged_data)} записей) сохранены в {filename}")
            
        except Exception as e:
            self.logger.log(f"Ошибка при сохранении объединенных данных: {e}", 'error')
            raise
    
    def save_dependencies(self, dependencies: List[Dict[str, str]], filename: str = 'dependencies.csv'):
        """Сохраняет зависимости в CSV файл"""
        if not dependencies:
            self.logger.log("Нет зависимостей для сохранения", 'warning')
            return
        
        fieldnames = ['node_external_id', 'prerequisite_external_id']
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for dep in dependencies:
                    writer.writerow(dep)
            
            self.logger.log(f"Зависимости ({len(dependencies)} записей) сохранены в {filename}")
            
        except Exception as e:
            self.logger.log(f"Ошибка при сохранении зависимостей: {e}", 'error')
            raise
    
    def run_full_merge(self, shop_file: str = 'shop.csv', 
                      merged_output: str = 'vehicles_merged.csv',
                      dependencies_output: str = 'dependencies.csv'):
        """
        Выполняет полный процесс объединения данных и создания зависимостей
        
        Args:
            shop_file: Входной файл с основными данными
            merged_output: Выходной файл для объединенных данных
            dependencies_output: Выходной файл для зависимостей
        """
        try:
            self.logger.log("Запуск полного процесса объединения данных...")
            
            # Шаг 1: Объединение данных
            merged_data = self.merge_shop_data(shop_file)
            if not merged_data:
                self.logger.log("Не удалось загрузить данные для объединения", 'error')
                return
            
            # Шаг 2: Сохранение объединенных данных
            self.save_merged_data(merged_data, merged_output)
            
            # Шаг 3: Извлечение зависимостей
            dependencies = self.extract_dependencies(merged_data)
            
            # Шаг 4: Сохранение зависимостей
            self.save_dependencies(dependencies, dependencies_output)
            
            self.logger.log("Полный процесс объединения данных завершен успешно!")
            
            # Итоговая статистика
            total_nodes = len(merged_data)
            total_deps = len(dependencies)
            coverage = (total_deps / total_nodes * 100) if total_nodes > 0 else 0
            
            self.logger.log(f"Итоговая статистика:")
            self.logger.log(f"  Узлов в дереве: {total_nodes}")
            self.logger.log(f"  Зависимостей: {total_deps}")
            self.logger.log(f"  Покрытие зависимостями: {coverage:.1f}%")
            
            return merged_data, dependencies
            
        except Exception as e:
            self.logger.log(f"Ошибка при выполнении полного процесса объединения: {e}", 'error')
            raise