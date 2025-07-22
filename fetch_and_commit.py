# fetch_and_commit.py

import os
import sys
import json
import requests
import gzip
import re
import argparse
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from collections import defaultdict

# --- Зависимости ---
try:
    from lxml import etree
except ImportError:
    sys.exit("Ошибка: lxml не установлен. Установите: pip install lxml")

# --- Конфигурация ---
MAX_WORKERS = 100
SOURCES_FILE = 'sources.json'
DATA_DIR = Path('data')
ICONS_DIR = Path('icons')
ICONS_MAP_FILE = Path('icons_map.json')
README_FILE = 'README.md'
RAW_BASE_URL = "https://raw.githubusercontent.com/{owner}/{repo}/main/{filepath}"

# --- Вспомогательные функции ---
def is_gzipped(file_path):
    """Проверяет, является ли файл gzipped."""
    with open(file_path, 'rb') as f:
        return f.read(2) == b'\x1f\x8b'

class CustomEncoder(json.JSONEncoder):
    """Класс для сериализации Path объектов и множеств в JSON."""
    def default(self, obj):
        if isinstance(obj, Path):
            return str(obj).replace('\\', '/') # Для совместимости с Windows
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

# --- ОСНОВНЫЕ ФУНКЦИИ ---

def read_sources_and_notes():
    """Читает источники и заметки из sources.json."""
    try:
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            sources, notes = config.get('sources', []), config.get('notes', '')
            if not sources:
                sys.exit("Ошибка: в sources.json нет источников.")
            return sources, notes
    except Exception as e:
        sys.exit(f"Ошибка чтения {SOURCES_FILE}: {e}")

def clear_directory(dir_path: Path):
    """Очищает указанную директорию от файлов и поддиректорий."""
    if dir_path.exists():
        for item in dir_path.iterdir():
            if item.is_dir():
                clear_directory(item)
                item.rmdir()
            else:
                item.unlink()
    else:
        dir_path.mkdir(parents=True, exist_ok=True)

def download_one(entry):
    """Скачивает один EPG файл."""
    url, desc = entry['url'], entry['desc']
    temp_path = DATA_DIR / ("tmp_" + os.urandom(4).hex())
    result = {'entry': entry, 'error': None}
    try:
        print(f"Начинаю загрузку: {desc} ({url})")
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(temp_path, 'wb') as f:
                for chunk in r.iter_content(16 * 1024):
                    f.write(chunk)
        size_bytes = temp_path.stat().st_size
        if size_bytes == 0:
            raise ValueError("Файл пустой.")
        result.update({'size_mb': round(size_bytes / (1024 * 1024), 2), 'temp_path': temp_path})
        return result
    except Exception as e:
        result['error'] = f"Ошибка загрузки: {e}"
        print(f"Ошибка для {desc}: {result['error']}")
        if temp_path.exists():
            temp_path.unlink()
    return result

def download_icon(session, url, save_path):
    """Скачивает одну иконку."""
    try:
        # Создаем родительские директории, если их нет
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with session.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        return True
    except requests.RequestException:
        return False

def get_icon_signature(file_path):
    """Создает 'сигнатуру' EPG-файла на основе URL-ов его иконок."""
    icon_urls = set()
    try:
        open_func = gzip.open if is_gzipped(file_path) else open
        with open_func(file_path, 'rb') as f:
            for _, element in etree.iterparse(f, tag='icon', events=('end',)):
                if 'src' in element.attrib:
                    icon_urls.add(element.attrib['src'])
                element.clear()
        
        if not icon_urls:
            return None
            
        # Сортируем URL для консистентности хэша
        sorted_urls = sorted(list(icon_urls))
        return hashlib.sha256(''.join(sorted_urls).encode('utf-8')).hexdigest()
    except Exception as e:
        print(f"Ошибка при создании сигнатуры для {file_path.name}: {e}", file=sys.stderr)
        return None

def perform_full_update(download_results):
    """Выполняет полное обновление: группирует источники, скачивает иконки, создает карту."""
    print("\n--- Этап 1: Группировка источников по наборам иконок ---")
    
    # Группируем источники по сигнатуре иконок
    groups = defaultdict(list)
    for res in download_results:
        if not res.get('error'):
            signature = get_icon_signature(res['temp_path'])
            # Если сигнатуры нет (нет иконок), источник будет в группе None
            groups[signature].append(res)
    
    print(f"Найдено {len(groups)} уникальных групп источников (включая группу без иконок).")

    icon_data = {
        "groups": {},
        "source_to_group": {}
    }
    urls_to_download = {}

    print("\n--- Этап 2: Создание карт иконок и подготовка к загрузке ---")
    for signature, sources_in_group in groups.items():
        # Пропускаем группу источников без иконок
        if signature is None:
            for res in sources_in_group:
                icon_data["source_to_group"][res['entry']['url']] = None
            continue
        
        group_id = signature[:12]
        group_icon_dir = ICONS_DIR / f"group_{group_id}"
        
        # Создаем карту сопоставления ID канала -> локальный путь
        icon_map_for_group = {}
        # Берем первый файл из группы для парсинга, т.к. наборы иконок у них одинаковые
        representative_file = sources_in_group[0]['temp_path']
        
        try:
            open_func = gzip.open if is_gzipped(representative_file) else open
            with open_func(representative_file, 'rb') as f:
                for _, channel in etree.iterparse(f, tag='channel', events=('end',)):
                    channel_id = channel.get('id')
                    icon_tag = channel.find('icon')
                    if channel_id and icon_tag is not None and 'src' in icon_tag.attrib:
                        icon_url = icon_tag.get('src')
                        # Генерируем имя файла из URL или ID канала
                        filename = Path(urlparse(icon_url).path).name or f"{channel_id}.png"
                        local_path = group_icon_dir / filename
                        icon_map_for_group[channel_id] = local_path
                        urls_to_download[icon_url] = local_path
                    channel.clear()
        except Exception as e:
            print(f"Ошибка парсинга файла-представителя {representative_file.name}: {e}", file=sys.stderr)
            continue # Пропускаем эту группу, если не удалось распарсить

        # Сохраняем информацию о группе
        icon_data["groups"][signature] = {
            "icon_dir": group_icon_dir,
            "icon_map": icon_map_for_group
        }
        for res in sources_in_group:
            icon_data["source_to_group"][res['entry']['url']] = signature
            
        print(f"Группа {group_id}: {len(sources_in_group)} источников, {len(icon_map_for_group)} иконок.")

    # Скачиваем все уникальные иконки
    print(f"\nТребуется скачать {len(urls_to_download)} уникальных иконок.")
    if urls_to_download:
        adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
        with requests.Session() as session:
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                downloader = partial(download_icon, session)
                future_to_url = {executor.submit(downloader, url, path): url for url, path in urls_to_download.items()}
                for future in as_completed(future_to_url):
                    future.result()
        print("Загрузка иконок завершена.")
        
    # Сохраняем карту
    print(f"Сохранение карты иконок в {ICONS_MAP_FILE}...")
    with open(ICONS_MAP_FILE, 'w', encoding='utf-8') as f:
        json.dump(icon_data, f, ensure_ascii=False, indent=2, cls=CustomEncoder)
    print("Карта иконок сохранена.")
    
    return icon_data

def load_icon_data_for_daily_update():
    """Загружает существующую карту иконок для ежедневного обновления."""
    print("\n--- Этап 1: Загрузка существующей карты иконок ---")
    if not ICONS_MAP_FILE.is_file():
        print(f"Файл {ICONS_MAP_FILE} не найден. Иконки не будут заменены.")
        print("Рекомендуется запустить полное обновление (`--full-update`) для его создания.")
        return None
    try:
        with open(ICONS_MAP_FILE, 'r', encoding='utf-8') as f:
            icon_data = json.load(f)
        
        # Преобразуем строковые пути обратно в Path объекты для удобства
        for group in icon_data.get('groups', {}).values():
            if 'icon_map' in group:
                group['icon_map'] = {k: Path(v) for k, v in group['icon_map'].items()}
        
        print(f"Карта иконок успешно загружена. Групп: {len(icon_data.get('groups', {}))}.")
        return icon_data
    except Exception as e:
        print(f"Ошибка загрузки или парсинга {ICONS_MAP_FILE}: {e}", file=sys.stderr)
        return None

def process_epg_file(file_path, icon_sub_map, owner, repo_name, entry):
    """Обрабатывает один EPG-файл, заменяя URL иконок по точной карте."""
    print(f"Обрабатываю файл: {file_path.name} для источника {entry['desc']}")
    if not icon_sub_map:
        print(f"Для {file_path.name} нет карты иконок. Пропускаю замену.")
        return True

    try:
        was_gzipped = is_gzipped(file_path)
        open_func = gzip.open if was_gzipped else open
        parser = etree.XMLParser(remove_blank_text=True)
        with open_func(file_path, 'rb') as f:
            tree = etree.parse(f, parser)
        root = tree.getroot()
        
        changes_made = 0
        for channel in root.findall('channel'):
            channel_id = channel.get('id')
            # Точный поиск по ID канала в карте для данной группы
            matched_icon_path = icon_sub_map.get(channel_id)
            
            if matched_icon_path:
                new_icon_url = RAW_BASE_URL.format(owner=owner, repo=repo_name, filepath=str(matched_icon_path).replace('\\', '/'))
                icon_tag = channel.find('icon')
                if icon_tag is None:
                    icon_tag = etree.SubElement(channel, 'icon')
                
                if icon_tag.get('src') != new_icon_url:
                    icon_tag.set('src', new_icon_url)
                    changes_made += 1
        
        if changes_made > 0:
            print(f"Внесено {changes_made} изменений в иконки файла {file_path.name}.")
            doctype_str = '<!DOCTYPE tv SYSTEM "https://iptvx.one/xmltv.dtd">'
            xml_bytes = etree.tostring(tree, pretty_print=True, xml_declaration=True, encoding='UTF-8', doctype=doctype_str)
            
            original_filename = Path(urlparse(entry['url']).path).name
            if original_filename.lower().endswith('.gz'):
                archive_internal_name = original_filename[:-3]
            else:
                archive_internal_name = f"{original_filename}.xml"
            
            if was_gzipped:
                with gzip.GzipFile(filename=archive_internal_name, mode='wb', fileobj=open(file_path, 'wb'), mtime=0) as f_out:
                    f_out.write(xml_bytes)
            else:
                with open(file_path, 'wb') as f_out:
                    f_out.write(xml_bytes)
        else:
            print(f"Изменений в иконках для {file_path.name} не требуется.")
        
        return True
    except Exception as e:
        print(f"Критическая ошибка при обработке {file_path}: {e}", file=sys.stderr)
        return False

def update_readme(results, notes):
    """Обновляет README.md с результатами выполнения."""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M %Z')
    lines = [notes, "\n---"] if notes else []
    lines.append(f"\n# Обновлено: {timestamp}\n")
    
    for idx, r in enumerate(results, 1):
        lines.append(f"### {idx}. {r['entry']['desc']}\n")
        if r.get('error'):
            lines.extend([f"**Статус:** 🔴 Ошибка", f"**Источник:** `{r['entry']['url']}`", f"**Причина:** {r.get('error')}", "---"])
        else:
            lines.extend([f"**Размер:** {r['size_mb']} MB", "", "**Ссылка для плеера (GitHub Raw):**", f"`{r['raw_url']}`", "---"])
    
    with open(README_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
    print(f"\nREADME.md обновлён ({len(results)} записей).")

def main():
    parser = argparse.ArgumentParser(description="EPG Updater Script")
    parser.add_argument('--full-update', action='store_true', help='Perform a full update, including icons and icon map.')
    args = parser.parse_args()

    repo = os.getenv('GITHUB_REPOSITORY')
    if not repo or '/' not in repo:
        sys.exit("Ошибка: Переменная окружения GITHUB_REPOSITORY не определена.")
    owner, repo_name = repo.split('/')
    
    sources, notes = read_sources_and_notes()
    
    print("\n--- Этап 0: Загрузка EPG файлов ---")
    clear_directory(DATA_DIR)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        download_results = list(executor.map(download_one, sources))

    icon_data = None
    if args.full_update:
        print("\nЗапущен режим ПОЛНОГО ОБНОВЛЕНИЯ.")
        clear_directory(ICONS_DIR)
        icon_data = perform_full_update(download_results)
    else:
        print("\nЗапущен режим ЕЖЕДНЕВНОГО ОБНОВЛЕНИЯ.")
        icon_data = load_icon_data_for_daily_update()

    print("\n--- Этап 3: Замена ссылок на иконки в EPG файлах ---")
    if icon_data:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for res in download_results:
                if res.get('error'): continue
                
                source_url = res['entry']['url']
                group_hash = icon_data['source_to_group'].get(source_url)
                
                icon_sub_map = {}
                if group_hash:
                    icon_sub_map = icon_data['groups'][group_hash].get('icon_map', {})
                
                futures.append(executor.submit(process_epg_file, res['temp_path'], icon_sub_map, owner, repo_name, res['entry']))
            
            for future in as_completed(futures):
                future.result()
    else:
        print("Данные об иконках отсутствуют, замена ссылок пропущена.")

    print("\n--- Этап 4: Финализация и обновление README ---")
    url_to_result = {res['entry']['url']: res for res in download_results}
    ordered_results = [url_to_result[s['url']] for s in sources]
    final_results, used_names = [], set()

    for res in ordered_results:
        if res.get('error'):
            final_results.append(res)
            continue
            
        final_filename_from_url = Path(urlparse(res['entry']['url']).path).name
        # Для URL без расширения (типа EPG_LITE) добавляем расширение
        if not Path(final_filename_from_url).suffix:
            ext = '.xml.gz' if is_gzipped(res['temp_path']) else '.xml'
            proposed_filename = f"{final_filename_from_url}{ext}"
        else:
            proposed_filename = final_filename_from_url

        # Обработка дубликатов имен файлов
        final_name, counter = proposed_filename, 1
        while final_name in used_names:
            p_stem = Path(proposed_filename).stem
            p_suffixes = "".join(Path(proposed_filename).suffixes)
            final_name = f"{p_stem}-{counter}{p_suffixes}"
            counter += 1
        used_names.add(final_name)
        
        target_path = DATA_DIR / final_name
        res['temp_path'].rename(target_path)
        res['raw_url'] = RAW_BASE_URL.format(owner=owner, repo=repo_name, filepath=str(target_path).replace('\\', '/'))
        final_results.append(res)

    update_readme(final_results, notes)
    print("\nСкрипт успешно завершил работу.")

if __name__ == '__main__':
    main()
