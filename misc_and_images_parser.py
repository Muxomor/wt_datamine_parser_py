import json
import csv
import requests
import re
from typing import Dict, List, Any, Optional

from utils import Config, Logger, Constants


class MiscAndImagesParser:
    """Класс для парсинга требований по рангам и изображений стран"""
    
    def __init__(self, config_path: str = 'config.txt'):
        """
        Инициализация парсера misc данных
        
        Args:
            config_path: Путь к конфигурационному файлу
        """
        self.config = Config(config_path)
        self.logger = Logger('misc_and_images_parser', 'misc_and_images_parser_debug.log')
        
    def fetch_rank_data(self) -> str:
        """Загружает данные rank.blkx из удаленного источника"""
        rank_url = self.config.get('rank_url')
        if not rank_url:
            raise ValueError("rank_url не найден в конфигурации")
            
        try:
            self.logger.log(f"Загрузка данных rank.blkx из: {rank_url}")
            response = requests.get(rank_url, timeout=30)
            response.raise_for_status()
            
            self.logger.log("Данные rank.blkx успешно загружены")
            return response.text
            
        except requests.RequestException as e:
            raise RuntimeError(f"Ошибка загрузки данных rank.blkx: {e}")
    
    def process_rank_data(self, raw_data: str) -> List[Dict[str, Any]]:
        """Обрабатывает данные rank.blkx и извлекает требования по рангам"""
        try:
            self.logger.log("Парсинг данных rank.blkx...")
            data = json.loads(raw_data)
            
            results = []
            pattern = re.compile(r"needBuyToOpenNextInEra([A-Za-z]+)(\d+)")
            era_data = data.get("needBuyToOpenNextInEra", {})
            
            for country_key, reqs in era_data.items():
                nation = country_key.replace("country_", "")
                for req_key, required_units in reqs.items():
                    if required_units == 0:
                        continue
                        
                    match = pattern.match(req_key)
                    if match:
                        raw_type, number_str = match.groups()
                        try:
                            prev_rank = int(number_str)
                        except ValueError:
                            continue
                            
                        target_rank = prev_rank + 1
                        vehicle_type = Constants.RANK_TYPE_MAPPING.get(raw_type)
                        
                        if not vehicle_type:
                            continue
                            
                        results.append({
                            "nation": nation,
                            "vehicle_type": vehicle_type,
                            "target_rank": target_rank,
                            "previous_rank": prev_rank,
                            "required_units": required_units
                        })
                        
                        self.logger.log(f"  Извлечено требование: {nation} {vehicle_type} ранг {target_rank} требует {required_units} юнитов", 'debug')
            
            self.logger.log(f"Извлечено требований по рангам: {len(results)}")
            return results
            
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Ошибка декодирования JSON rank.blkx: {e}")
        except Exception as e:
            raise RuntimeError(f"Ошибка обработки данных rank.blkx: {e}")
    
    def fetch_country_flags(self) -> List[Dict[str, str]]:
        """Собирает данные о флагах стран"""
        self.logger.log("Сбор данных о флагах стран...")
        
        country_flags = []
        total_countries = len(Constants.COUNTRY_MAPPING)
        found_count = 0
        not_found_count = 0
        
        for country_key, country_code in Constants.COUNTRY_MAPPING.items():
            flag_url = f"{Constants.FLAGS_BASE_URL}country_{country_code}.svg"
            
            # Проверяем доступность флага
            try:
                self.logger.log(f"  Проверка флага для {country_code}: {flag_url}", 'debug')
                response = requests.head(flag_url, timeout=10)
                
                if response.status_code == 200:
                    found_count += 1
                    self.logger.log(f"  Флаг найден: {country_code}", 'debug')
                    country_flags.append({
                        'country': country_code,
                        'flag_image_url': flag_url
                    })
                else:
                    not_found_count += 1
                    self.logger.log(f"  Флаг недоступен: {country_code} (статус: {response.status_code})", 'warning')
                    country_flags.append({
                        'country': country_code,
                        'flag_image_url': ''  # Заглушка для недоступных флагов
                    })
                    
            except requests.RequestException as e:
                not_found_count += 1
                self.logger.log(f"  Ошибка при проверке флага {country_code}: {e}", 'warning')
                country_flags.append({
                    'country': country_code,
                    'flag_image_url': ''  # Заглушка при ошибке
                })
        
        self.logger.log(f"Статистика флагов стран:")
        self.logger.log(f"  Найдено: {found_count}")
        self.logger.log(f"  Недоступно: {not_found_count}")
        self.logger.log(f"  Всего обработано: {total_countries}")
        
        return country_flags
    
    def save_rank_requirements_to_csv(self, data: List[Dict[str, Any]], filename: str = 'rank_requirements.csv'):
        """Сохраняет требования по рангам в CSV файл"""
        if not data:
            self.logger.log("Нет данных требований по рангам для сохранения", 'warning')
            return
            
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=Constants.RANK_REQUIREMENTS_CSV_FIELDNAMES)
                writer.writeheader()
                
                for item in data:
                    writer.writerow(item)
                    
            self.logger.log(f"Требования по рангам ({len(data)} записей) сохранены в {filename}")
            
        except Exception as e:
            self.logger.log(f"Ошибка при сохранении требований по рангам в CSV: {e}", 'error')
            raise
    
    def save_country_flags_to_csv(self, data: List[Dict[str, str]], filename: str = 'country_flags.csv'):
        """Сохраняет данные о флагах стран в CSV файл"""
        if not data:
            self.logger.log("Нет данных о флагах стран для сохранения", 'warning')
            return
            
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=Constants.COUNTRY_FLAGS_CSV_FIELDNAMES)
                writer.writeheader()
                
                for item in data:
                    writer.writerow(item)
                    
            self.logger.log(f"Данные о флагах стран ({len(data)} записей) сохранены в {filename}")
            
        except Exception as e:
            self.logger.log(f"Ошибка при сохранении флагов стран в CSV: {e}", 'error')
            raise
    
    def debug_github_api(self):
        """Отладочный метод для анализа GitHub API"""
        try:
            available_images = self._fetch_github_images_list()
            
            print(f"\n=== DEBUG: GitHub API ===")
            print(f"Всего файлов получено: {len(available_images)}")
            
            # Показываем первые 20 файлов
            print("\nПервые 20 файлов:")
            for i, (key, url) in enumerate(list(available_images.items())[:20]):
                print(f"  {key} -> {url}")
            
            # Ищем конкретные примеры из shop.csv
            test_ids = [
                'us_m2a4', 'us_m3_stuart', 'us_sherman_group', 
                'us_m4a1_1942_sherman', 'us_sherman_76w_group'
            ]
            print(f"\nПроверка тестовых ID из shop.csv:")
            for test_id in test_ids:
                if test_id in available_images:
                    print(f"  ✅ {test_id} НАЙДЕН -> {available_images[test_id]}")
                else:
                    print(f"  ❌ {test_id} НЕ НАЙДЕН")
                    # Ищем похожие по частям имени
                    parts = test_id.replace('_', ' ').replace('-', ' ').split()
                    similar = []
                    for key in available_images.keys():
                        if any(part in key for part in parts if len(part) > 2):
                            similar.append(key)
                    if similar:
                        print(f"     Похожие: {similar[:5]}")
                        
            return available_images
        except Exception as e:
            print(f"❌ Ошибка отладки GitHub API: {e}")
            return {}
    
    def debug_search_strategies(self, unit_id: str, available_images: Dict[str, str]):
        """Отладочный метод для анализа стратегий поиска"""
        print(f"\n=== DEBUG: Поиск для {unit_id} ===")
        
        # Генерируем все стратегии поиска
        search_variants = [
            unit_id,  # Точное совпадение
            unit_id.replace('-', '_'),  # Заменяем дефисы на подчеркивания
            unit_id.replace('_', '-'),  # Заменяем подчеркивания на дефисы  
        ]
        
        # Дополнительные варианты для групп
        if unit_id_lower.endswith('_group'):
            base_name = unit_id_lower[:-6]  # Убираем '_group'
            search_variants.extend([
                f"{base_name.replace('-', '')}_group",  # Убираем дефисы из базового имени
                f"{base_name.replace('_', '-')}_group",  # Заменяем подчеркивания на дефисы
            ])
                
            # Пробуем добавить дефис в разных позициях для групп
            if len(base_name) >= 3:
                search_variants.extend([
                    f"{base_name[:2]}-{base_name[2:]}_group",  # he51 -> he-51
                    f"{base_name[:3]}-{base_name[3:]}_group",  # p26 -> p-26
                ])
        
        # Убираем дубликаты, сохраняя порядок
        unique_variants = []
        for variant in search_variants:
            if variant not in unique_variants:
                unique_variants.append(variant)
        
        print(f"Пробуем варианты: {unique_variants}")
        
        # Проверяем каждый вариант
        found = False
        for variant in unique_variants:
            if variant in available_images:
                print(f"  ✅ НАЙДЕН: {variant} -> {available_images[variant]}")
                found = True
                break
            else:
                print(f"  ❌ НЕ НАЙДЕН: {variant}")
        
        if not found:
            # Ищем частично похожие
            parts = unit_id.replace('_', ' ').replace('-', ' ').split()
            similar = []
            for key in available_images.keys():
                score = sum(1 for part in parts if part in key and len(part) > 2)
                if score > 0:
                    similar.append((key, score))
            
            # Сортируем по количеству совпадений
            similar.sort(key=lambda x: x[1], reverse=True)
            if similar:
                print(f"  Возможные совпадения:")
                for key, score in similar[:5]:
                    print(f"    {key} (совпадений: {score})")
        
        return found

    def _load_image_fields_fallback(self, shop_images_fields_path: str = 'shop_images_fields.csv') -> Dict[str, str]:
        """Загружает fallback данные из полей image shop.blkx"""
        image_fields = {}
        
        try:
            with open(shop_images_fields_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    unit_id = row.get('id', '').strip().lower()
                    image_field = row.get('image_field', '').strip()
                    if unit_id and image_field:
                        image_fields[unit_id] = image_field
                        
            self.logger.log(f"Загружено {len(image_fields)} fallback полей image")
            return image_fields
            
        except FileNotFoundError:
            self.logger.log(f"Файл {shop_images_fields_path} не найден. Fallback недоступен.", 'warning')
            return {}
        except Exception as e:
            self.logger.log(f"Ошибка чтения fallback файла {shop_images_fields_path}: {e}", 'warning')
            return {}
    
    def fetch_shop_images(self, shop_csv_path: str = 'shop.csv', shop_images_fields_path: str = 'shop_images_fields.csv') -> List[Dict[str, str]]:
        """Ищет изображение используя поле image из shop.blkx как fallback"""
        unit_id_lower = unit_id.lower()
        
        if unit_id_lower in image_fields:
            image_name = image_fields[unit_id_lower]
            fallback_url = f"{Constants.IMAGES_BASE_URL}{image_name}.png"
            
            self.logger.log(f"    FALLBACK: {unit_id} -> {image_name} -> {fallback_url}", 'debug')
            return fallback_url
        
        return ''
        """Собирает изображения для ID из shop.csv используя GitHub API"""
        self.logger.log("Сбор изображений техники...")
        
        # Загружаем ID из shop.csv
        shop_ids = []
        try:
            with open(shop_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    unit_id = row.get('id', '').strip()
                    if unit_id:
                        shop_ids.append(unit_id)
                        
            self.logger.log(f"Загружено {len(shop_ids)} ID из {shop_csv_path}")
            
        except FileNotFoundError:
            raise RuntimeError(f"Файл {shop_csv_path} не найден. Сначала выполните основной парсинг shop.blkx")
        except Exception as e:
            raise RuntimeError(f"Ошибка чтения файла {shop_csv_path}: {e}")
        
        # Получаем список файлов изображений из GitHub
        available_images = self._fetch_github_images_list()
        
        # Загружаем fallback данные из полей image
        image_fields_fallback = self._load_image_fields_fallback(shop_images_fields_path)
        
        # ДОПОЛНИТЕЛЬНАЯ ДИАГНОСТИКА: тестируем прямой доступ к CDN
        self.test_direct_cdn_access()
        
        # ОТЛАДКА: анализируем первые несколько ID
        self.logger.log("=== ОТЛАДКА: Анализ первых 5 ID ===")
        debug_ids = shop_ids[:5]
        for debug_id in debug_ids:
            found = self.debug_search_strategies(debug_id, available_images)
            if not found:
                self.logger.log(f"ПРОБЛЕМНЫЙ ID: {debug_id}")
        
        # Обрабатываем изображения
        shop_images = []
        found_count = 0
        fallback_count = 0
        not_found_count = 0
        total_ids = len(shop_ids)
        problem_ids = []
        
        for idx, unit_id in enumerate(shop_ids, 1):
            if idx % 100 == 0:
                self.logger.log(f"  Обработано: {idx}/{total_ids}")
                
            # Сначала обычный поиск
            image_url = self._find_image_for_id(unit_id, available_images)
            
            # Если не найдено - пробуем fallback
            if not image_url and image_fields_fallback:
                image_url = self._find_image_from_shop_field(unit_id, image_fields_fallback)
                if image_url:
                    fallback_count += 1
                    self.logger.log(f"  FALLBACK изображение: {unit_id} -> {image_url}", 'debug')
            
            if image_url:
                found_count += 1
                self.logger.log(f"  Найдено изображение: {unit_id} -> {image_url}", 'debug')
            else:
                not_found_count += 1
                problem_ids.append(unit_id)
                self.logger.log(f"  Изображение не найдено: {unit_id}", 'debug')
            
            shop_images.append({
                'id': unit_id,
                'image_url': image_url
            })
        
        # Дополнительная отладочная информация
        self.logger.log(f"Статистика изображений техники:")
        self.logger.log(f"  Найдено обычным поиском: {found_count - fallback_count}")
        self.logger.log(f"  Найдено через fallback: {fallback_count}")
        self.logger.log(f"  Всего найдено: {found_count}")
        self.logger.log(f"  Недоступно: {not_found_count}")
        self.logger.log(f"  Всего обработано: {total_ids}")
        self.logger.log(f"  Процент успеха: {(found_count/total_ids)*100:.1f}%")
        
        # Показываем первые 10 проблемных ID для анализа
        if problem_ids:
            self.logger.log(f"\nПервые 10 проблемных ID:")
            for pid in problem_ids[:10]:
                self.logger.log(f"  - {pid}")
        
        return shop_images
    
    def _fetch_github_images_list(self) -> Dict[str, str]:
        """Получает список всех файлов изображений из GitHub репозитория используя Tree API"""
        self.logger.log("Загрузка списка изображений из GitHub...")
        
        # Используем Tree API для получения ВСЕХ файлов
        github_tree_api_url = f"{Constants.GITHUB_API_BASE_URL}/repos/gszabi99/War-Thunder-Datamine/git/trees/master?recursive=1"
        
        try:
            self.logger.log(f"Отправляем запрос к GitHub Tree API: {github_tree_api_url}")
            response = requests.get(github_tree_api_url, timeout=60)  # Увеличиваем таймаут
            self.logger.log(f"Получен ответ от GitHub Tree API. Status: {response.status_code}, Size: {len(response.content)} bytes")
            response.raise_for_status()
            
            tree_data = response.json()
            all_files = tree_data.get('tree', [])
            self.logger.log(f"GitHub Tree API вернул {len(all_files)} элементов")
            
            # Фильтруем файлы из нужной папки
            target_path = "atlases.vromfs.bin_u/units/"
            available_images = {}
            png_files_count = 0
            total_files_in_target = 0
            
            # Ищем конкретно наши проблемные файлы
            problem_files = ['us_m2a4.png', 'us_m3_stuart.png', 'us_m24_chaffee.png']
            found_problem_files = []
            
            for file_info in all_files:
                file_path = file_info.get('path', '')
                
                # Проверяем что файл из нужной папки
                if file_path.startswith(target_path):
                    total_files_in_target += 1
                    filename = file_path[len(target_path):]  # Убираем путь, оставляем имя файла
                    
                    if file_info.get('type') == 'blob' and filename.endswith('.png'):
                        filename_without_ext = filename[:-4]  # Убираем .png
                        
                        # Формируем CDN URL
                        cdn_url = f"{Constants.IMAGES_BASE_URL}{filename}"
                        available_images[filename_without_ext] = cdn_url
                        png_files_count += 1
                        
                        # Проверяем проблемные файлы
                        if filename in problem_files:
                            found_problem_files.append(filename)
                            self.logger.log(f"НАЙДЕН проблемный файл: {filename}")
                        
                        self.logger.log(f"    Файл: {filename_without_ext} -> {cdn_url}", 'debug')
            
            self.logger.log(f"ДИАГНОСТИКА GitHub Tree API:")
            self.logger.log(f"  Всего элементов в дереве: {len(all_files)}")
            self.logger.log(f"  Файлов в целевой папке: {total_files_in_target}")
            self.logger.log(f"  PNG файлов: {png_files_count}")
            self.logger.log(f"  Проблемных файлов найдено: {len(found_problem_files)} из {len(problem_files)}")
            self.logger.log(f"  Найденные проблемные файлы: {found_problem_files}")
            
            # Показываем первые 10 PNG файлов для проверки
            first_10_png = list(available_images.keys())[:10]
            self.logger.log(f"  Первые 10 PNG файлов: {first_10_png}")
            
            # Проверяем есть ли наши тестовые файлы
            test_files = ['us_m2a4', 'us_m3_stuart', 'us_m24_chaffee']
            found_test_files = []
            for test_file in test_files:
                if test_file.lower() in available_images:  # Ищем в lowercase
                    found_test_files.append(test_file)
            
            self.logger.log(f"  Тестовых файлов найдено в словаре: {len(found_test_files)} из {len(test_files)}")
            self.logger.log(f"  Найденные тестовые файлы: {found_test_files}")
            self.logger.log(f"  Все ключи в lowercase для совместимости")
            
            return available_images
            
        except requests.Timeout as e:
            self.logger.log(f"ТАЙМАУТ при запросе к GitHub Tree API: {e}", 'error')
            raise RuntimeError(f"Таймаут при получении списка файлов из GitHub: {e}")
        except requests.RequestException as e:
            self.logger.log(f"СЕТЕВАЯ ОШИБКА при запросе к GitHub Tree API: {e}", 'error')
            raise RuntimeError(f"Ошибка при получении списка файлов из GitHub: {e}")
        except Exception as e:
            self.logger.log(f"ОБЩАЯ ОШИБКА при обработке данных GitHub Tree API: {e}", 'error')
            raise RuntimeError(f"Ошибка обработки данных GitHub Tree API: {e}")
    
    def test_direct_cdn_access(self):
        """Тестовый метод для проверки прямого доступа к CDN"""
        test_ids = ['us_m2a4', 'us_m3_stuart', 'us_m24_chaffee']
        
        self.logger.log("\n=== ТЕСТ: Прямой доступ к CDN ===")
        
        for test_id in test_ids:
            cdn_url = f"{Constants.IMAGES_BASE_URL}{test_id}.png"
            try:
                self.logger.log(f"Проверяем прямой доступ: {cdn_url}")
                response = requests.head(cdn_url, timeout=10)
                
                if response.status_code == 200:
                    self.logger.log(f"  ✅ CDN ДОСТУПЕН: {test_id} -> {response.status_code}")
                else:
                    self.logger.log(f"  ❌ CDN НЕДОСТУПЕН: {test_id} -> {response.status_code}")
                    
            except requests.Timeout as e:
                self.logger.log(f"  ⏰ ТАЙМАУТ CDN: {test_id} -> {e}")
            except requests.RequestException as e:
                self.logger.log(f"  🔥 ОШИБКА CDN: {test_id} -> {e}")
                
        self.logger.log("=== КОНЕЦ ТЕСТА CDN ===\n")
    
    def _find_image_for_id(self, unit_id: str, available_images: Dict[str, str]) -> str:
        """Ищет изображение для конкретного ID в списке доступных файлов"""
        
        # Приводим ID к нижнему регистру для поиска
        unit_id_lower = unit_id.lower()
        
        # Стратегии поиска (по приоритету)
        search_variants = [
            unit_id_lower,  # Точное совпадение
            unit_id_lower.replace('-', '_'),  # Заменяем дефисы на подчеркивания
            unit_id_lower.replace('_', '-'),  # Заменяем подчеркивания на дефисы  
        ]
        
        # Дополнительные варианты для групп
        if unit_id.endswith('_group'):
            base_name = unit_id[:-6]  # Убираем '_group'
            search_variants.extend([
                f"{base_name.replace('-', '')}_group",  # Убираем дефисы из базового имени
                f"{base_name.replace('_', '-')}_group",  # Заменяем подчеркивания на дефисы
            ])
            
            # Пробуем добавить дефис в разных позициях для групп
            if len(base_name) >= 3:
                search_variants.extend([
                    f"{base_name[:2]}-{base_name[2:]}_group",  # he51 -> he-51
                    f"{base_name[:3]}-{base_name[3:]}_group",  # p26 -> p-26
                ])
        
        # Убираем дубликаты, сохраняя порядок
        unique_variants = []
        for variant in search_variants:
            if variant not in unique_variants:
                unique_variants.append(variant)
        
        # Ищем совпадения
        for variant in unique_variants:
            if variant in available_images:
                self.logger.log(f"    Найдено: {unit_id} -> {variant} -> {available_images[variant]}", 'debug')
                return available_images[variant]
            else:
                self.logger.log(f"    Вариант не найден: {variant}", 'debug')
        
        # Если ничего не найдено
        self.logger.log(f"    Изображение не найдено для: {unit_id}", 'debug')
        return ''
    
    def save_shop_images_to_csv(self, data: List[Dict[str, str]], filename: str = 'shop_images.csv'):
        """Сохраняет данные об изображениях техники в CSV файл"""
        if not data:
            self.logger.log("Нет данных об изображениях техники для сохранения", 'warning')
            return
            
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=Constants.SHOP_IMAGES_CSV_FIELDNAMES)
                writer.writeheader()
                
                for item in data:
                    writer.writerow(item)
                    
            self.logger.log(f"Данные об изображениях техники ({len(data)} записей) сохранены в {filename}")
            
        except Exception as e:
            self.logger.log(f"Ошибка при сохранении изображений техники в CSV: {e}", 'error')
            raise
    
    def run(self, rank_output_file: str = 'rank_requirements.csv', flags_output_file: str = 'country_flags.csv', 
            images_output_file: str = 'shop_images.csv', shop_csv_path: str = 'shop.csv'):
        """Основной метод запуска парсера misc данных"""
        try:
            self.logger.log("Запуск парсера misc данных...")
            
            # Обрабатываем требования по рангам
            try:
                rank_raw_data = self.fetch_rank_data()
                rank_data = self.process_rank_data(rank_raw_data)
                self.save_rank_requirements_to_csv(rank_data, rank_output_file)
                self.logger.log("Обработка требований по рангам завершена успешно!")
            except Exception as e:
                self.logger.log(f"Ошибка при обработке требований по рангам: {e}", 'error')
                self.logger.log("Продолжаем с обработкой флагов стран...", 'warning')
            
            # Обрабатываем флаги стран
            try:
                flags_data = self.fetch_country_flags()
                self.save_country_flags_to_csv(flags_data, flags_output_file)
                self.logger.log("Обработка флагов стран завершена успешно!")
            except Exception as e:
                self.logger.log(f"Ошибка при обработке флагов стран: {e}", 'error')
                self.logger.log("Продолжаем с обработкой изображений техники...", 'warning')
            
            # Обрабатываем изображения техники
            try:
                images_data = self.fetch_shop_images(shop_csv_path)
                self.save_shop_images_to_csv(images_data, images_output_file)
                self.logger.log("Обработка изображений техники завершена успешно!")
            except Exception as e:
                self.logger.log(f"Ошибка при обработке изображений техники: {e}", 'error')
            
            self.logger.log(f"Парсинг misc данных завершен!")
            self.logger.log(f"Созданные файлы: {rank_output_file}, {flags_output_file}, {images_output_file}")
            return True
            
        except Exception as e:
            self.logger.log(f"Критическая ошибка при выполнении парсинга misc данных: {e}", 'error')
            raise