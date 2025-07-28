import logging
import sys
from typing import Dict, Any


class Config:
    """Класс для работы с конфигурацией приложения"""
    
    def __init__(self, config_path: str = 'config.txt'):
        self.config_path = config_path
        self.config = self._read_config()
        
    def _read_config(self) -> Dict[str, str]:
        """Читает конфигурационный файл"""
        config = {}
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if '=' not in line:
                            print(f"Предупреждение: Пропускаем некорректную строку #{line_num}: {line}")
                            continue
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip()
        except FileNotFoundError:
            raise RuntimeError(f"Конфигурационный файл '{self.config_path}' не найден.")
        except Exception as e:
            raise RuntimeError(f"Ошибка чтения конфига '{self.config_path}': {str(e)}")
        
        return config
    
    def get(self, key: str, default: Any = None) -> Any:
        """Получает значение из конфигурации"""
        return self.config.get(key, default)


class Logger:
    """Класс для настройки и управления логированием"""
    
    def __init__(self, name: str = 'shop_parser', log_file: str = 'shop_parser_debug.log'):
        self.logger = logging.getLogger(name)
        self._setup_logging(log_file)
    
    def _setup_logging(self, log_file: str):
        """Настройка логирования в файл и консоль"""
        self.logger.setLevel(logging.DEBUG)
        
        # Очищаем существующие обработчики
        self.logger.handlers.clear()
        
        # Обработчик для файла
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
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


class Constants:
    """Константы приложения"""
    
    # Порог для определения премиумной колонки (0.X = X0% премиумной техники)
    PREMIUM_THRESHOLD = 0.3
    
    # Обрабатывать ли slave-юниты (для будущего развития)
    PROCESS_SLAVE_UNITS = False
    
    # Маппинг типов техники из JSON в название для БД
    VEHICLE_TYPE_MAPPING = {
        'army': 'Наземная техника',
        'aviation': 'Авиация', 
        'helicopters': 'Вертолёты',
        'ships': 'Большой флот',
        'boats': 'Малый флот'
    }
    
    # Маппинг стран
    COUNTRY_MAPPING = {
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
    
    # Маппинг типов техники для требований по рангам
    RANK_TYPE_MAPPING = {
        "Aircraft": "Авиация",
        "Helicopter": "Вертолёты", 
        "Tank": "Наземная техника",
        "Ship": "Большой флот",
        "Boat": "Малый флот"
    }
    
    # Базовый URL для флагов стран
    FLAGS_BASE_URL = 'https://wiki.warthunder.ru/static/country_svg/'
    
    # Базовый URL для изображений техники
    IMAGES_BASE_URL = 'https://static.encyclopedia.warthunder.com/slots/'
    
    # GitHub API базовый URL
    GITHUB_API_BASE_URL = 'https://api.github.com'
    
    # Служебные поля для определения групп
    SERVICE_FIELDS = {
        'rank', 'reqAir', 'image', 'slaveUnit', 'reqUnlock', 'gift', 
        'marketplaceItemdefId', 'hideFeature', 'event', 'showOnlyWhenBought',
        'beginPurchaseDate', 'endPurchaseDate', 'isClanVehicle', 'reqFeature',
        'showByPlatform', 'costGold', 'freeRepairs', 'rankPosXY', 'fakeReqUnitType',
        'fakeReqUnitImage', 'fakeReqUnitRank', 'fakeReqUnitPosXY', 'showOnlyWhenResearch',
        'hideByPlatform'
    }
    
    # Признаки премиумной техники
    PREMIUM_INDICATORS = [
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
    
    # Аномальные окончания для удаления
    ANOMALOUS_SUFFIXES = ['_race', '_football', '_yt_cup_2019', '_event', '_naval']
    
    # НОВОЕ: Аномальные начала для удаления
    ANOMALOUS_PREFIXES = ['ucav_']
    
    # Поля для CSV экспорта
    CSV_FIELDNAMES = [
        'id', 'rank', 'country', 'vehicle_type', 'type', 'status',
        'column_index', 'row_index', 'predecessor', 'order_in_folder'
    ]
    
    # Поля для CSV локализации
    LOCALIZATION_CSV_FIELDNAMES = [
        'id', 'localized_name'
    ]
    
    # Поля для CSV wpcost
    WPCOST_CSV_FIELDNAMES = [
        'id', 'silver', 'exp', 'br'
    ]
    
    # Поля для CSV требований по рангам
    RANK_REQUIREMENTS_CSV_FIELDNAMES = [
        'nation', 'vehicle_type', 'target_rank', 'previous_rank', 'required_units'
    ]
    
    # Поля для CSV флагов стран  
    COUNTRY_FLAGS_CSV_FIELDNAMES = [
        'country', 'flag_image_url'
    ]
    
    # Поля для CSV изображений техники
    SHOP_IMAGES_CSV_FIELDNAMES = [
        'id', 'image_url'
    ]