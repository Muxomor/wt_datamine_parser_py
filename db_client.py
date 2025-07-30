import csv
import requests
import jwt
import time
from typing import Dict, List, Optional, Any
from utils import Constants

class PostgrestClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None, jwt_secret: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.trust_env = False
        
        headers = {'Content-Type': 'application/json'}
        
        # Добавляем аутентификацию для парсера
        if api_key and jwt_secret:
            # Создаем JWT токен для парсера
            payload = {
                'role': 'parser_role',
                'aud': 'postgrest',
                'exp': int(time.time()) + 3600  # 1 час
            }
            token = jwt.encode(payload, jwt_secret, algorithm='HS256')
            headers['Authorization'] = f'Bearer {token}'
            print("✅ Парсер авторизован с JWT токеном")
        elif api_key:
            # Fallback на простой API ключ
            headers['Authorization'] = f'Bearer {api_key}'
            print("✅ Парсер авторизован с API ключом")
        else:
            print("⚠️  Работа без аутентификации (только чтение)")
            
        self.session.headers.update(headers)

    def delete_all(self, table: str):
        """Удаление всех записей из таблицы"""
        url = f"{self.base_url}/{table}"
        r = self.session.delete(url)
        r.raise_for_status()
        print(f"✅ Очищена таблица {table}")
        return r.status_code

    def _post(self, path: str, data: List[Dict[str, Any]]):
        """POST запрос"""
        url = f"{self.base_url}/{path}"
        r = self.session.post(url, json=data)
        r.raise_for_status()
        if r.text:
            try:
                return r.json()
            except ValueError:
                return r.status_code
        return r.status_code

    def _get(self, path: str, params: Optional[Dict[str, str]] = None):
        """GET запрос"""
        url = f"{self.base_url}/{path}"
        r = self.session.get(url, params=params)
        r.raise_for_status()
        return r.json()

    def _patch(self, path: str, data: Dict[str, Any]):
        """PATCH запрос"""
        url = f"{self.base_url}/{path}"
        r = self.session.patch(url, json=data)
        r.raise_for_status()
        if r.text:
            try:
                return r.json()
            except ValueError:
                return r.status_code
        return r.status_code

    def upsert_vehicle_types(self, vehicle_types: List[str]):
        """Вставка типов техники"""
        payload = [{'name': vt} for vt in vehicle_types]
        result = self._post('vehicle_types', payload)
        print(f"✅ Загружено {len(vehicle_types)} типов техники")
        return result

    def upsert_nations(self, nations: List[Dict[str, str]]):
        """Вставка наций"""
        result = self._post('nations', nations)
        print(f"✅ Загружено {len(nations)} наций")
        return result

    def fetch_map(self, table: str, key_field: str = 'name') -> Dict[str, int]:
        """Получение справочника key -> id"""
        data = self._get(table, params={'select': f"id,{key_field}"})
        mapping = {rec[key_field]: rec['id'] for rec in data}
        print(f"✅ Загружен справочник {table}: {len(mapping)} записей")
        return mapping

    def insert_nodes(self, nodes_payload: List[Dict[str, Any]]):
        """Вставка узлов техники"""
        return self._post('nodes', nodes_payload)

    def insert_node_dependencies(self, deps_payload: List[Dict[str, Any]]):
        """Вставка зависимостей между узлами"""
        result = self._post('node_dependencies', deps_payload)
        print(f"✅ Загружено {len(deps_payload)} зависимостей")
        return result

    def insert_rank_requirements(self, reqs_payload: List[Dict[str, Any]]):
        """Вставка требований по рангам"""
        result = self._post('rank_requirements', reqs_payload)
        print(f"✅ Загружено {len(reqs_payload)} требований по рангам")
        return result
    
    def test_connection(self):
        """Тест подключения и прав доступа"""
        try:
            # Тест чтения
            response = self._get("nodes", params={'limit': '1'})
            print("✅ Чтение работает")
            
            # Тест записи (создание и удаление тестовой записи)
            test_nation = {
                "name": "TEST_NATION_DELETE_ME",  
                "image_url": "test.png"
            }
            
            try:
                self._post("nations", [test_nation])
                print("✅ Запись работает")
                # Удаляем тестовую запись
                self.session.delete(f"{self.base_url}/nations?name=eq.TEST_NATION_DELETE_ME")
            except Exception as e:
                print(f"❌ Ошибка записи: {e}")
                
        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")


def upload_all_data(config: Dict[str, str],
                    merged_csv: str = "vehicles_merged.csv",
                    deps_csv: str = "dependencies.csv",
                    rank_csv: str = "rank_requirements.csv",
                    country_csv: str = "country_flags.csv"):
    """
    Полная заливка данных через PostgREST с аутентификацией парсера
    """
    base_url = config.get('base_url')
    api_key = config.get('parser_api_key')
    jwt_secret = config.get('jwt_secret')
    
    if not base_url:
        raise ValueError("В config не указан base_url для PostgREST")
    
    if not api_key:
        print("⚠️  ВНИМАНИЕ: parser_api_key не указан в config")
    
    if not jwt_secret:
        print("⚠️  ВНИМАНИЕ: jwt_secret не указан в config")
    
    # Создаем клиент с JWT токеном
    client = PostgrestClient(base_url, api_key, jwt_secret)
    
    # Тестируем подключение
    print("🔍 Тестирование подключения...")
    client.test_connection()
    
    print("\n🚀 Начинаем загрузку данных...")

    # 1) Очистка всех таблиц в правильном порядке
    print("\n🗑️  Очистка таблиц...")
    for tbl in ['node_dependencies', 'rank_requirements', 'nodes', 'nations', 'vehicle_types']:
        try:
            client.delete_all(tbl)
        except Exception as e:
            print(f"❌ Ошибка очистки таблицы {tbl}: {e}")
            raise

    # 2) Читаем merged CSV для извлечения уникальных типов техники
    print(f"\n📊 Читаю данные из {merged_csv}...")
    try:
        with open(merged_csv, 'r', encoding='utf-8') as f:
            merged_data = list(csv.DictReader(f))
        print(f"📊 Найдено {len(merged_data)} записей для обработки")
    except FileNotFoundError:
        print(f"❌ Файл {merged_csv} не найден")
        raise

    # 3) Извлекаем уникальные типы техники в правильном порядке
    # Определяем правильный порядок типов техники
    vehicle_types_order = [
        'Авиация',
        'Наземная техника', 
        'Вертолёты',
        'Малый флот',
        'Большой флот'
    ]

    # Проверяем какие типы есть в данных
    vehicle_types_in_data = set(row['vehicle_category'] for row in merged_data if row.get('vehicle_category'))

    # Берем только те типы, которые есть в данных, в правильном порядке
    vehicle_types = [vt for vt in vehicle_types_order if vt in vehicle_types_in_data]
    
    print(f"\n📝 Заливаю vehicle_types: {vehicle_types}")
    client.upsert_vehicle_types(vehicle_types)

    # 4) nations из country_flags.csv
    print(f"\n🏳️  Заливаю nations из {country_csv}...")
    nations_payload = []
    try:
        with open(country_csv, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                nations_payload.append({
                    'name': row['country'].strip(),
                    'image_url': row['flag_image_url'].strip()
                })
        client.upsert_nations(nations_payload)
    except FileNotFoundError:
        print(f"❌ Файл {country_csv} не найден")
        raise

    # 5) Получаем справочники
    print("\n📋 Загружаю справочники...")
    vt_map = client.fetch_map('vehicle_types', key_field='name')
    nat_map = client.fetch_map('nations', key_field='name')

    # 6) Подготавливаем узлы для вставки
    nodes_payload = []
    
    for nd in merged_data:
        external_id = nd.get('external_id', '').strip()
        if not external_id:
            print(f"⚠️  Пропущена запись без external_id: {nd}")
            continue

        country_key = nd.get('country', '').strip()
        if country_key not in nat_map:
            print(f"⚠️  Узел {external_id}: неизвестная страна '{country_key}'")
            continue

        vehicle_category = nd.get('vehicle_category', '').strip()
        if vehicle_category not in vt_map:
            print(f"⚠️  Узел {external_id}: неизвестный vehicle_type '{vehicle_category}'")
            continue

        # Безопасное преобразование чисел
        def safe_int(value):
            if value is None or value == '':
                return None
            try:
                return int(value)
            except (ValueError, TypeError):
                return None

        def safe_float(value):
            if value is None or value == '':
                return None
            try:
                return float(str(value).replace(',', '.'))
            except (ValueError, TypeError):
                return None

        nodes_payload.append({
            'external_id': external_id,
            'name': nd.get('name') or external_id,
            'type': nd.get('type', 'vehicle'),
            'tech_category': nd.get('tech_category', 'standard'),
            'nation_id': nat_map[country_key],
            'vehicle_type_id': vt_map[vehicle_category],
            'rank': safe_int(nd.get('rank')),
            'silver_cost': safe_int(nd.get('silver')),
            'required_exp': safe_int(nd.get('required_exp')),
            'image_url': nd.get('image_url') or None,
            'br': safe_float(nd.get('battle_rating')),
            'column_index': safe_int(nd.get('column')),
            'row_index': safe_int(nd.get('row')),
            'order_in_folder': safe_int(nd.get('order_in_folder')),
        })

    # 7) Вставляем узлы батчами
    print(f"\n🚗 Вставка {len(nodes_payload)} узлов...")
    batch_size = 100
    for i in range(0, len(nodes_payload), batch_size):
        batch = nodes_payload[i:i + batch_size]
        try:
            client.insert_nodes(batch)
            print(f"📊 Обработано {min(i + batch_size, len(nodes_payload))}/{len(nodes_payload)} записей")
        except Exception as e:
            print(f"❌ Ошибка вставки батча {i//batch_size + 1}: {e}")
            # Пробуем вставить по одной записи для диагностики
            for idx, rec in enumerate(batch):
                try:
                    client.insert_nodes([rec])
                except Exception as single_e:
                    print(f"❌ Ошибка вставки узла {rec['external_id']}: {single_e}")
                    raise

    # 8) Обновление parent_id
    print("\n🔗 Обновление parent_id...")
    node_map = client.fetch_map('nodes', key_field='external_id')
    updated_count = 0
    
    for nd in merged_data:
        external_id = nd.get('external_id', '').strip()
        parent_external_id = nd.get('parent_external_id', '').strip()
        
        if external_id in node_map and parent_external_id and parent_external_id in node_map:
            try:
                client._patch(f"nodes?external_id=eq.{external_id}",
                              {'parent_id': node_map[parent_external_id]})
                updated_count += 1
            except Exception as e:
                print(f"⚠️  Ошибка обновления parent_id для {external_id}: {e}")
    
    print(f"✅ Обновлено {updated_count} связей parent_id")

    # 9) node_dependencies
    print(f"\n🔗 Загрузка зависимостей из {deps_csv}...")
    deps = []
    
    try:
        with open(deps_csv, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                node_external_id = row.get('node_external_id', '').strip()
                prerequisite_external_id = row.get('prerequisite_external_id', '').strip()

                if node_external_id in node_map and prerequisite_external_id in node_map:
                    deps.append({
                        'node_id': node_map[node_external_id],
                        'prerequisite_node_id': node_map[prerequisite_external_id]
                    })
        
        if deps:
            client.insert_node_dependencies(deps)
        else:
            print("⚠️  Зависимости не найдены")
            
    except FileNotFoundError:
        print(f"⚠️  Файл {deps_csv} не найден, пропуск зависимостей")

    # 10) rank_requirements
    print(f"\n🎖️  Загрузка требований по рангам из {rank_csv}...")
    rr = []
    
    try:
        with open(rank_csv, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                nation_name = row.get('nation', '').strip()
                vehicle_type_name = row.get('vehicle_type', '').strip()
                
                if nation_name not in nat_map:
                    print(f"⚠️  Пропущено требование: неизвестная страна '{nation_name}'")
                    continue
                if vehicle_type_name not in vt_map:
                    print(f"⚠️  Пропущено требование: неизвестный тип '{vehicle_type_name}'")
                    continue
                    
                rr.append({
                    'nation_id': nat_map[nation_name],
                    'vehicle_type_id': vt_map[vehicle_type_name],
                    'target_rank': int(row['target_rank']),
                    'previous_rank': int(row['previous_rank']),
                    'required_units': int(row['required_units']),
                })
        
        if rr:
            client.insert_rank_requirements(rr)
        else:
            print("⚠️  Требования по рангам не найдены")
            
    except FileNotFoundError:
        print(f"⚠️  Файл {rank_csv} не найден, пропуск требований по рангам")

    print("\n🎉 Всё успешно загружено через PostgREST!")