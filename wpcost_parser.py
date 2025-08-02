import json
import csv
import requests
from typing import Dict, List, Any, Optional

from utils import Config, Logger


class WpcostParser:
    """Класс для парсинга данных wpcost.blkx War Thunder"""
    
    def __init__(self, config_path: str = 'config.txt'):
        """
        Инициализация парсера wpcost.blkx
        
        Args:
            config_path: Путь к конфигурационному файлу
        """
        self.config = Config(config_path)
        self.logger = Logger('wpcost_parser', 'wpcost_parser_debug.log')
        self.wpcost_data: Dict[str, Any] = {}
        
    def fetch_wpcost_data(self) -> Dict[str, Any]:
        """Загружает данные wpcost.blkx из удаленного источника с поддержкой fallback URL"""
        # Основной URL из конфигурации
        primary_url = self.config.get('wpcost_url')
        if not primary_url:
            raise ValueError("wpcost_url не найден в конфигурации")
        
        # Fallback URL для обхода ограничений jsdelivr
        fallback_urls = self.config.get('wpcost_fallback_urls', '').split(',')
        fallback_urls = [url.strip() for url in fallback_urls if url.strip()]
        
        # Список всех URL для попытки загрузки
        urls_to_try = [primary_url] + fallback_urls
        
        for attempt, url in enumerate(urls_to_try, 1):
            try:
                self.logger.log(f"Попытка {attempt}: Загрузка данных wpcost из: {url}")
                response = requests.get(url, timeout=45)
                
                # Проверяем статус код
                if response.status_code == 403:
                    self.logger.log(f"Получен код 403 (файл слишком большой или доступ запрещен) для URL: {url}", 'warning')
                    if attempt < len(urls_to_try):
                        self.logger.log("Пробуем следующий URL...", 'warning')
                        continue
                    else:
                        raise requests.RequestException(f"Все URL недоступны. Последняя ошибка: 403 Forbidden")
                
                response.raise_for_status()
                
                # Проверяем размер файла
                content_length = response.headers.get('content-length')
                if content_length:
                    size_mb = int(content_length) / (1024 * 1024)
                    self.logger.log(f"Размер файла: {size_mb:.1f} МБ")
                
                wpcost_data = response.json()
                self.logger.log(f"Данные wpcost успешно загружены с попытки {attempt}")
                return wpcost_data
                
            except requests.RequestException as e:
                self.logger.log(f"Ошибка загрузки с URL {url}: {e}", 'warning')
                if attempt < len(urls_to_try):
                    self.logger.log("Пробуем следующий URL...", 'warning')
                    continue
                else:
                    raise RuntimeError(f"Не удалось загрузить данные wpcost ни с одного URL. Последняя ошибка: {e}")
            except json.JSONDecodeError as e:
                self.logger.log(f"Ошибка декодирования JSON с URL {url}: {e}", 'warning')
                if attempt < len(urls_to_try):
                    self.logger.log("Пробуем следующий URL...", 'warning')
                    continue
                else:
                    raise RuntimeError(f"Ошибка декодирования JSON wpcost: {e}")
    
    def _is_premium_unit(self, status: str, have_prem_flag: str) -> bool:
        """
        Проверяет, является ли юнит премиумным
        
        Args:
            status: Статус юнита из CSV
            have_prem_flag: Флаг премиума из CSV
            
        Returns:
            True если юнит премиумный (нужно занулить silver и exp)
        """
        # Проверяем status = premium (регистр не важен)
        if status.lower() == 'premium':
            return True
            
        # Проверяем have_prem_flag = True (регистр не важен)
        if have_prem_flag.lower() == 'true':
            return True
            
        return False
    
    def load_shop_ids(self, shop_csv_path: str = 'shop.csv') -> Dict[str, Dict[str, str]]:
        """Загружает данные из файла shop.csv для всех юнитов"""
        shop_data = {}
        total_count = 0
        premium_count = 0
        
        try:
            with open(shop_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    total_count += 1
                    unit_id = row.get('id', '').strip()
                    status = row.get('status', '').strip()
                    have_prem_flag = row.get('have_prem_flag', '').strip()
                    
                    if not unit_id:
                        self.logger.log(f"Пропущена строка без ID: {row}", 'warning')
                        continue
                        
                    # Сохраняем данные юнита с нормализованным ID
                    normalized_id = unit_id.lower()
                    shop_data[normalized_id] = {
                        'original_id': unit_id,  # Сохраняем оригинальный ID для вывода
                        'status': status,
                        'have_prem_flag': have_prem_flag
                    }
                    
                    # Считаем премиумные юниты для статистики
                    if self._is_premium_unit(status, have_prem_flag):
                        premium_count += 1
                        
            self.logger.log(f"Статистика загрузки из {shop_csv_path}:")
            self.logger.log(f"  Всего строк в CSV: {total_count}")
            self.logger.log(f"  Премиумных юнитов: {premium_count}")
            self.logger.log(f"  Обычных юнитов: {total_count - premium_count}")
            
            return shop_data
            
        except FileNotFoundError:
            raise RuntimeError(f"Файл {shop_csv_path} не найден. Сначала выполните основной парсинг shop.blkx")
        except Exception as e:
            raise RuntimeError(f"Ошибка чтения файла {shop_csv_path}: {e}")
    
    def calculate_br(self, economic_rank: int) -> float:
        """
        Вычисляет БР (Battle Rating) из economicRankHistorical
        
        Args:
            economic_rank: Значение economicRankHistorical
            
        Returns:
            Округленное значение БР или 1.0 если входное значение некорректно
        """
        if economic_rank is None or economic_rank <= 0:
            self.logger.log(f"    БР расчет: economicRank некорректен ({economic_rank}), используем fallback 1.0", 'debug')
            return 1.0
            
        # Формула: БР = (economicRankHistorical / 3) + 1
        br_raw = (economic_rank / 3) + 1
        
        # Округляем до ближайшего значения: X.0, X.3, X.7
        integer_part = int(br_raw)
        decimal_part = br_raw - integer_part
        
        # Определяем возможные значения для округления
        possible_values = [
            integer_part,           # X.0
            integer_part + 0.3,     # X.3  
            integer_part + 0.7,     # X.7
            integer_part + 1        # (X+1).0
        ]
        
        # Находим ближайшее значение
        closest_value = min(possible_values, key=lambda x: abs(x - br_raw))
        
        self.logger.log(f"    БР расчет: economicRank={economic_rank}, raw={br_raw:.3f}, округлено={closest_value}", 'debug')
        
        return closest_value
    
    def extract_unit_data(self, unit_id: str, unit_data: Dict[str, Any], is_premium: bool = False) -> Dict[str, Any]:
        """
        Извлекает необходимые данные для одного юнита
        
        Args:
            unit_id: ID юнита (оригинальный из shop.csv)
            unit_data: Данные юнита из wpcost.blkx
            is_premium: True если юнит премиумный (нужно занулить silver и exp)
            
        Returns:
            Словарь с извлеченными данными
        """
        if is_premium:
            # Для премиумных юнитов обнуляем silver и exp, но BR рассчитываем
            silver = 0
            exp = None
            self.logger.log(f"  Премиумный юнит {unit_id}: silver=0, exp=None (обнулено)", 'debug')
        else:
            # Для обычных юнитов обрабатываем как раньше
            # Извлекаем значения с проверкой типов
            value = unit_data.get('value')
            req_exp = unit_data.get('reqExp')
            
            # Обрабатываем value (silver)
            if value is None or not isinstance(value, (int, float)):
                silver = 0
                self.logger.log(f"    value отсутствует или некорректно для {unit_id}, установлено 0", 'debug')
            else:
                silver = int(value)
            
            # Обрабатываем reqExp (exp)
            if req_exp is None or not isinstance(req_exp, (int, float)):
                exp = None
                self.logger.log(f"    reqExp отсутствует или некорректно для {unit_id}, установлено None", 'debug')
            elif req_exp == 0:
                exp = None  # Специальное условие: если reqExp = 0, записываем None
                self.logger.log(f"    reqExp равно 0 для {unit_id}, установлено None", 'debug')
            else:
                exp = int(req_exp)
        
        # BR рассчитываем в любом случае (и для премиумных, и для обычных)
        economic_rank = unit_data.get('economicRankHistorical')
        if economic_rank is None or not isinstance(economic_rank, (int, float)):
            br = 1.0  # Fallback значение
            self.logger.log(f"    economicRankHistorical отсутствует или некорректно для {unit_id}, BR установлен 1.0", 'debug')
        else:
            br = self.calculate_br(int(economic_rank))
        
        result = {
            'id': unit_id,  # Используем оригинальный ID для сохранения
            'silver': silver,
            'exp': exp,
            'br': br
        }
        
        premium_flag = " (премиум)" if is_premium else ""
        self.logger.log(f"  Обработан {unit_id}{premium_flag}: silver={silver}, exp={exp}, br={br}", 'debug')
        
        return result
    
    def normalize_wpcost_data(self, wpcost_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Нормализует ID в данных wpcost к нижнему регистру
        
        Args:
            wpcost_data: Исходные данные wpcost
            
        Returns:
            Данные с нормализованными ID
        """
        normalized_data = {}
        original_count = 0
        normalized_count = 0
        
        for original_id, unit_data in wpcost_data.items():
            # Пропускаем служебные поля
            if original_id.startswith('economicRank') or not isinstance(unit_data, dict):
                continue
                
            original_count += 1
            normalized_id = original_id.lower()
            
            # Проверяем, есть ли конфликт ID после нормализации
            if normalized_id in normalized_data:
                self.logger.log(f"Конфликт ID после нормализации: '{original_id}' и другой ID дают '{normalized_id}'", 'warning')
                # В случае конфликта сохраняем первый встреченный
                continue
            
            normalized_data[normalized_id] = unit_data
            normalized_count += 1
            
            # Логируем изменения регистра
            if original_id != normalized_id:
                self.logger.log(f"Нормализован ID: '{original_id}' -> '{normalized_id}'", 'debug')
        
        self.logger.log(f"Нормализация ID в wpcost:")
        self.logger.log(f"  Исходных ID: {original_count}")
        self.logger.log(f"  Нормализованных ID: {normalized_count}")
        self.logger.log(f"  Конфликтов: {original_count - normalized_count}")
        
        return normalized_data
    
    def process_wpcost_data(self, shop_data: Dict[str, Dict[str, str]], wpcost_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Обрабатывает данные wpcost.blkx для всех юнитов из shop.csv"""
        results = []
        found_count = 0
        not_found_count = 0
        premium_processed_count = 0
        
        self.logger.log("Начинаем обработку данных wpcost...")
        
        # Нормализуем ID в wpcost данных
        normalized_wpcost_data = self.normalize_wpcost_data(wpcost_data)
        
        self.logger.log(f"В wpcost найдено {len(normalized_wpcost_data)} юнитов после нормализации")
        
        for normalized_shop_id, unit_shop_data in shop_data.items():
            if normalized_shop_id in normalized_wpcost_data:
                # Юнит найден в wpcost данных
                unit_data = normalized_wpcost_data[normalized_shop_id]
                original_id = unit_shop_data['original_id']
                
                # Проверяем, является ли юнит премиумным
                is_premium = self._is_premium_unit(
                    unit_shop_data['status'], 
                    unit_shop_data['have_prem_flag']
                )
                
                if is_premium:
                    premium_processed_count += 1
                    self.logger.log(f"  Обрабатываем премиумный юнит: {original_id} (status={unit_shop_data['status']}, have_prem_flag={unit_shop_data['have_prem_flag']})", 'debug')
                
                # Используем оригинальный ID для сохранения результата
                extracted_data = self.extract_unit_data(original_id, unit_data, is_premium)
                results.append(extracted_data)
                found_count += 1
                
            else:
                # Юнит не найден в wpcost - пропускаем
                not_found_count += 1
                original_id = unit_shop_data['original_id']
                self.logger.log(f"  Не найден в wpcost: {original_id} (нормализован: {normalized_shop_id}) - пропускается", 'debug')
        
        self.logger.log(f"Статистика обработки wpcost:")
        self.logger.log(f"  Найдено и обработано: {found_count}")
        self.logger.log(f"  Из них премиумных (с обнуленными silver/exp): {premium_processed_count}")
        self.logger.log(f"  Не найдено в wpcost (пропущено): {not_found_count}")
        self.logger.log(f"  Всего ID из shop.csv: {len(shop_data)}")
        
        return results
    
    def save_to_csv(self, data: List[Dict[str, Any]], filename: str = 'wpcost.csv'):
        """Сохраняет данные wpcost в CSV файл"""
        if not data:
            self.logger.log("Нет данных wpcost для сохранения", 'warning')
            return
            
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['id', 'silver', 'exp', 'br']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for item in data:
                    # Подготавливаем данные для записи
                    row_data = {
                        'id': item['id'],
                        'silver': item['silver'],
                        'exp': item['exp'] if item['exp'] is not None else '',
                        'br': item['br']  # БР всегда будет числом (минимум 1.0)
                    }
                    writer.writerow(row_data)
                    
            self.logger.log(f"Данные wpcost ({len(data)} записей) сохранены в {filename}")
            
            # Дополнительная статистика
            with_exp_count = sum(1 for item in data if item['exp'] is not None)
            fallback_br_count = sum(1 for item in data if item['br'] == 1.0)
            
            self.logger.log(f"Статистика данных:")
            self.logger.log(f"  Записей с опытом (exp): {with_exp_count}")
            self.logger.log(f"  Записей с fallback БР (1.0): {fallback_br_count}")
            self.logger.log(f"  Всего записей: {len(data)}")
            
        except Exception as e:
            self.logger.log(f"Ошибка при сохранении wpcost в CSV: {e}", 'error')
            raise
    
    def run(self, shop_csv_path: str = 'shop.csv', output_file: str = 'wpcost.csv'):
        """Основной метод запуска парсера wpcost"""
        try:
            self.logger.log("Запуск парсера wpcost.blkx...")
            
            # Загружаем данные из shop.csv (для всех юнитов)
            shop_data = self.load_shop_ids(shop_csv_path)
            
            # Загружаем данные wpcost.blkx
            wpcost_data = self.fetch_wpcost_data()
            
            # Обрабатываем данные
            processed_data = self.process_wpcost_data(shop_data, wpcost_data)
            
            # Сохраняем результат
            self.save_to_csv(processed_data, output_file)
            
            self.logger.log(f"Парсинг wpcost завершен успешно! Создан файл {output_file}")
            return True
            
        except Exception as e:
            self.logger.log(f"Ошибка при выполнении парсинга wpcost: {e}", 'error')
            raise