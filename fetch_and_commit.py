
import os
import sys
import json
import requests
import gzip
import re
import argparse
import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from collections import defaultdict

try:
    from lxml import etree
except ImportError:
    sys.exit("Ошибка: lxml не установлен. Установите: pip install lxml")

MAX_WORKERS = 25
REQUEST_TIMEOUT = 60
ICON_TIMEOUT = 20

SOURCES_FILE = 'sources.json'
DATA_DIR = Path('data')
ICONS_DIR = Path('icons')
ICONS_MAP_FILE = Path('icons_map.json')
README_FILE = 'README.md'
RAW_BASE_URL = "https://raw.githubusercontent.com/{owner}/{repo}/main/{filepath}"


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Path): return str(obj).replace('\\', '/')
        if isinstance(obj, set): return list(obj)
        return json.JSONEncoder.default(self, obj)

def is_gzipped(file_path):
    try:
        with open(file_path, 'rb') as f: return f.read(2) == b'\x1f\x8b'
    except IOError: return False

def read_sources_and_notes():
    try:
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            return config.get('sources', []), config.get('notes', '')
    except Exception as e: sys.exit(f"❌ Ошибка чтения {SOURCES_FILE}: {e}")

def clear_directory(dir_path: Path):
    if dir_path.exists():
        for item in dir_path.iterdir():
            try:
                if item.is_dir():
                    clear_directory(item)
                    item.rmdir()
                else: item.unlink()
            except Exception as e: print(f"⚠️  Предупреждение: не удалось удалить {item}: {e}")
    else: dir_path.mkdir(parents=True, exist_ok=True)

def download_one(entry):
    url, desc = entry['url'], entry['desc']
    temp_path = DATA_DIR / ("tmp_" + os.urandom(4).hex())
    result = {'entry': entry, 'error': None}
    try:
        print(f"🔄 Загружаю EPG: {desc}")
        with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
            r.raise_for_status()
            with open(temp_path, 'wb') as f:
                for chunk in r.iter_content(32 * 1024): f.write(chunk)
        size_mb = round(temp_path.stat().st_size / (1024 * 1024), 2)
        if size_mb == 0: raise ValueError("Файл пустой.")
        print(f"✅ EPG загружен: {desc} ({size_mb} MB)")
        result.update({'size_mb': size_mb, 'temp_path': temp_path})
        return result
    except Exception as e:
        result['error'] = f"Ошибка загрузки EPG: {e}"
        print(f"❌ Ошибка для {desc}: {result['error']}")
        if temp_path.exists(): temp_path.unlink()
    return result

def download_icon(session, url, save_path):
    if save_path.exists() and save_path.stat().st_size > 0: return 'skipped'
    try:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with session.get(url, stream=True, timeout=ICON_TIMEOUT) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(8192): f.write(chunk)
        return 'downloaded'
    except requests.RequestException: return 'failed'

def get_icon_signature(file_path):
    icon_urls = set()
    try:
        open_func = gzip.open if is_gzipped(file_path) else open
        with open_func(file_path, 'rb') as f:
            for _, element in etree.iterparse(f, tag='icon', events=('end',)):
                if 'src' in element.attrib: icon_urls.add(element.attrib['src'])
                element.clear()
        if not icon_urls: return None
        sorted_urls = sorted(list(icon_urls))
        return hashlib.sha256(''.join(sorted_urls).encode('utf-8')).hexdigest()
    except Exception as e:
        print(f"⚠️  Ошибка создания сигнатуры для {file_path.name}: {e}")
        return None

def perform_full_update(download_results):
    print("\n--- Этап 1: Группировка источников ---")
    groups = defaultdict(list)
    for res in download_results:
        if not res.get('error'):
            print(f"🔍 Анализирую: {res['entry']['desc']}")
            signature = get_icon_signature(res['temp_path'])
            groups[signature].append(res)
    print(f"✅ Найдено {len(groups)} уникальных групп источников.")

    icon_data = {"icon_pool": {}, "groups": {}, "source_to_group": {}}
    all_unique_urls = set()

    print("\n--- Этап 2: Создание карт иконок ---")
    for signature, sources_in_group in groups.items():
        if signature is None:
            for res in sources_in_group: icon_data["source_to_group"][res['entry']['url']] = None
            continue
        
        icon_map_for_group = {}
        representative_file = sources_in_group[0]['temp_path']
        try:
            open_func = gzip.open if is_gzipped(representative_file) else open
            with open_func(representative_file, 'rb') as f:
                for _, channel in etree.iterparse(f, tag='channel', events=('end',)):
                    channel_id = channel.get('id')
                    icon_tag = channel.find('icon')
                    if channel_id and icon_tag is not None and 'src' in icon_tag.attrib:
                        icon_url = icon_tag.get('src')
                        icon_map_for_group[channel_id] = icon_url
                        all_unique_urls.add(icon_url)
                    channel.clear()
        except Exception as e:
            print(f"⚠️  Ошибка парсинга {representative_file.name}: {e}")
            continue

        icon_data["groups"][signature] = {"icon_map": icon_map_for_group}
        for res in sources_in_group: icon_data["source_to_group"][res['entry']['url']] = signature
        print(f"   - Группа {signature[:8]}...: {len(icon_map_for_group)} иконок-ссылок.")
    
    icon_pool_dir = ICONS_DIR / "pool"
    urls_to_download = {}
    print(f"\n--- Этап 2.1: Подготовка к загрузке {len(all_unique_urls)} уникальных иконок ---")
    for url in all_unique_urls:
        url_hash = hashlib.sha1(url.encode('utf-8')).hexdigest()
        ext = "".join(Path(urlparse(url).path).suffixes) or ".png"
        pool_path = icon_pool_dir / f"{url_hash}{ext}"
        icon_data["icon_pool"][url] = pool_path
        urls_to_download[url] = pool_path

    if urls_to_download:
        print(f"📥 Требуется скачать/проверить: {len(urls_to_download)} иконок")
        downloaded, skipped, failed = 0, 0, 0
        total = len(urls_to_download)
        
        adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
        with requests.Session() as session:
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_url = {executor.submit(download_icon, session, url, path): url for url, path in urls_to_download.items()}
                
                for i, future in enumerate(as_completed(future_to_url), 1):
                    status = future.result()
                    if status == 'downloaded': downloaded += 1
                    elif status == 'skipped': skipped += 1
                    else: failed += 1
                    
                    if i % 100 == 0 or i == total:
                        print(f"   📊 Прогресс: {i}/{total} | Новых: {downloaded} | Пропущено: {skipped} | Ошибок: {failed}", end="\r")
        print("\n✅ Загрузка иконок завершена.")

    print(f"\n💾 Сохранение карты иконок...")
    with open(ICONS_MAP_FILE, 'w', encoding='utf-8') as f:
        json.dump(icon_data, f, ensure_ascii=False, indent=2, cls=CustomEncoder)
    print("✅ Карта иконок сохранена.")
    return icon_data

def load_icon_data_for_daily_update():
    print("\n--- Этап 1: Загрузка существующей карты иконок ---")
    if not ICONS_MAP_FILE.is_file():
        print(f"📁 Файл {ICONS_MAP_FILE} не найден.")
        return None
    try:
        with open(ICONS_MAP_FILE, 'r', encoding='utf-8') as f: icon_data = json.load(f)
        if 'icon_pool' in icon_data: icon_data['icon_pool'] = {k: Path(v) for k, v in icon_data['icon_pool'].items()}
        print(f"✅ Карта иконок загружена.")
        return icon_data
    except Exception as e:
        print(f"❌ Ошибка загрузки {ICONS_MAP_FILE}: {e}")
        return None

def process_epg_file(file_path, group_map, icon_pool, owner, repo_name, entry):
    print(f"🔧 Обрабатываю: {entry['desc']}")
    if not group_map or not icon_pool:
        print(f"   - Пропущено: нет карты иконок.")
        return True
    try:
        was_gzipped = is_gzipped(file_path)
        open_func = gzip.open if was_gzipped else open
        parser = etree.XMLParser(remove_blank_text=True, recover=True)
        with open_func(file_path, 'rb') as f: tree = etree.parse(f, parser)
        root = tree.getroot()
        changes_made = 0
        for channel in root.findall('channel'):
            channel_id = channel.get('id')
            icon_url_pointer = group_map.get(channel_id)
            if icon_url_pointer:
                matched_icon_path = icon_pool.get(icon_url_pointer)
                if matched_icon_path and matched_icon_path.exists():
                    new_icon_url = RAW_BASE_URL.format(owner=owner, repo=repo_name, filepath=str(matched_icon_path).replace('\\', '/'))
                    icon_tag = channel.find('icon')
                    if icon_tag is None: icon_tag = etree.SubElement(channel, 'icon')
                    if icon_tag.get('src') != new_icon_url:
                        icon_tag.set('src', new_icon_url)
                        changes_made += 1
        if changes_made > 0:
            print(f"   - Внесено изменений: {changes_made}")
            doctype = '<!DOCTYPE tv SYSTEM "https://iptvx.one/xmltv.dtd">'
            xml_bytes = etree.tostring(tree, pretty_print=True, xml_declaration=True, encoding='UTF-8', doctype=doctype)
            original_filename = Path(urlparse(entry['url']).path).name
            internal_name = original_filename[:-3] if original_filename.lower().endswith('.gz') else f"{original_filename}.xml"
            if was_gzipped:
                with gzip.GzipFile(filename=internal_name, mode='wb', fileobj=open(file_path, 'wb'), mtime=0) as f: f.write(xml_bytes)
            else:
                with open(file_path, 'wb') as f: f.write(xml_bytes)
        else: print(f"   - Изменений не требуется.")
        return True
    except Exception as e:
        print(f"❌ Ошибка обработки {file_path.name}: {e}")
        return False

def update_readme(results, notes):
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M %Z')
    lines = [notes, "\n---"] if notes else []
    lines.append(f"\n# 🔄 Обновлено: {timestamp}\n")
    for idx, r in enumerate(results, 1):
        lines.append(f"**{idx}. {r['entry']['desc']}**\n")
        if r.get('error'):
            lines.extend([f"**Статус:** ❌ Ошибка", f"`{r.get('error')}`", "\n---"])
        else:
            lines.extend([f"**Размер:** {r['size_mb']} MB", "", "**Ссылка:**", f"`{r['raw_url']}`", "\n---"])
    with open(README_FILE, 'w', encoding='utf-8') as f: f.write("\n".join(lines))
    print(f"✅ README.md обновлён.")

def main():
    parser = argparse.ArgumentParser(description="EPG Updater Script")
    parser.add_argument('--full-update', action='store_true', help='Выполнить полное обновление')
    args = parser.parse_args()

    repo = os.getenv('GITHUB_REPOSITORY')
    if not repo or '/' not in repo: sys.exit("❌ Ошибка: GITHUB_REPOSITORY не определена.")
    owner, repo_name = repo.split('/')
    print(f"🚀 Запуск EPG Updater для {owner}/{repo_name}")

    sources, notes = read_sources_and_notes()

    print("\n--- Этап 0: Загрузка EPG файлов ---")
    clear_directory(DATA_DIR)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        download_results = list(executor.map(download_one, sources))

    icon_data = None
    if args.full_update:
        print("\n✨ Режим: ПОЛНОЕ ОБНОВЛЕНИЕ")
        clear_directory(ICONS_DIR)
        icon_data = perform_full_update(download_results)
    else:
        print("\n📅 Режим: ЕЖЕДНЕВНОЕ ОБНОВЛЕНИЕ")
        icon_data = load_icon_data_for_daily_update()

    print("\n--- Этап 3: Замена ссылок на иконки ---")
    if icon_data:
        icon_pool = icon_data.get('icon_pool', {})
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for res in download_results:
                if res.get('error'): continue
                source_url = res['entry']['url']
                group_hash = icon_data['source_to_group'].get(source_url)
                group_map = {}
                if group_hash and group_hash in icon_data.get('groups', {}):
                    group_map = icon_data['groups'][group_hash].get('icon_map', {})
                futures.append(executor.submit(process_epg_file, res['temp_path'], group_map, icon_pool, owner, repo_name, res['entry']))
            for future in as_completed(futures): future.result()
    else: print("ℹ️  Данные об иконках отсутствуют, замена пропущена.")

    print("\n--- Этап 4: Финализация ---")
    url_to_result = {res['entry']['url']: res for res in download_results}
    ordered_results = [url_to_result[s['url']] for s in sources]
    final_results, used_names = [], set()

    for res in ordered_results:
        if res.get('error'):
            final_results.append(res)
            continue
        
        final_filename = Path(urlparse(res['entry']['url']).path).name
        if not Path(final_filename).suffix:
            ext = '.xml.gz' if is_gzipped(res['temp_path']) else '.xml'
            final_filename = f"{final_filename}{ext}"
        
        counter = 1
        proposed_name = final_filename
        while proposed_name in used_names:
            stem, suffix = Path(final_filename).stem, "".join(Path(final_filename).suffixes)
            proposed_name = f"{stem}-{counter}{suffix}"
            counter += 1
        used_names.add(proposed_name)
        
        target_path = DATA_DIR / proposed_name
        try:
            res['temp_path'].rename(target_path)
            res['raw_url'] = RAW_BASE_URL.format(owner=owner, repo=repo_name, filepath=str(target_path).replace('\\', '/'))
        except Exception as e: res['error'] = f"Ошибка перемещения файла: {e}"
        final_results.append(res)

    update_readme(final_results, notes)
    print("\n🎉 Скрипт успешно завершил работу!")

if __name__ == '__main__':
    main()
