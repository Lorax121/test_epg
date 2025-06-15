# fetch_and_commit.py

import os
import sys
import json
import requests
import gzip
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

# Используем lxml для производительного парсинга
try:
    from lxml import etree
except ImportError:
    print("Ошибка: библиотека lxml не установлена. Пожалуйста, установите ее: pip install lxml", file=sys.stderr)
    sys.exit(1)

# Используем thefuzz для быстрого сравнения строк
try:
    from thefuzz import fuzz
except ImportError:
    print("Ошибка: библиотека thefuzz не установлена. Пожалуйста, установите ее: pip install thefuzz python-Levenshtein", file=sys.stderr)
    sys.exit(1)

import gdshortener

# --- КОНФИГУРАЦИЯ ОПТИМИЗАЦИИ ---
# GitHub Actions runner имеет 2 CPU, но много I/O, ставим больше потоков для сетевых операций
MAX_WORKERS = 50
# Порог схожести названий каналов (80%)
SIMILARITY_THRESHOLD = 80 # thefuzz использует шкалу 0-100

# --- КОНФИГУРАЦИЯ ---
SOURCES_FILE = 'sources.json'
DATA_DIR = Path('data')
ICONS_DIR = Path('icons')
README_FILE = 'README.md'
CHUNK_SIZE = 16 * 1024
MAX_FILE_SIZE_MB = 95
JSDELIVR_SIZE_LIMIT_MB = 20

RAW_BASE_URL = "https://raw.githubusercontent.com/{owner}/{repo}/main/{filepath}"
JSDELIVR_BASE_URL = "https://cdn.jsdelivr.net/gh/{owner}/{repo}@main/{filepath}"

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def clean_name(name):
    """Очищает имя канала для лучшего сравнения."""
    name = name.lower()
    name = re.sub(r'\s*\b(hd|fhd|uhd|4k|8k|sd|low|vip|\(p\))\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\[.*?\]|\(.*?\)', '', name)
    name = re.sub(r'[^\w\s]', '', name)
    return ' '.join(name.split())

def get_channel_names(channel_element):
    """Извлекает и очищает все display-name из элемента channel."""
    names = [el.text for el in channel_element.findall('display-name')]
    return {clean_name(name) for name in names if name}

def is_gzipped(file_path):
    """Проверяет, является ли файл gzipped, по его магическим байтам."""
    with open(file_path, 'rb') as f:
        return f.read(2) == b'\x1f\x8b'

# --- ОСНОВНЫЕ ФУНКЦИИ ---

def read_sources_and_notes():
    # ... (код без изменений) ...
    try:
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            sources = config.get('sources', [])
            notes = config.get('notes', '')
            if not sources:
                print("Ошибка: в sources.json не найдено ни одного источника в ключе 'sources'.", file=sys.stderr)
                sys.exit(1)
            for s in sources:
                s.setdefault('ico_src', False)
            return sources, notes
    except FileNotFoundError:
        print(f"Ошибка: Файл {SOURCES_FILE} не найден.", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Ошибка: Некорректный формат JSON в файле {SOURCES_FILE}.", file=sys.stderr)
        sys.exit(1)


def clear_dirs():
    # ... (код без изменений) ...
    for d in [DATA_DIR, ICONS_DIR]:
        if d.exists():
            for f in d.iterdir():
                if f.is_file(): f.unlink()
        else:
            d.mkdir(parents=True, exist_ok=True)
    gitignore = ICONS_DIR / '.gitignore'
    if not gitignore.exists():
        gitignore.write_text('*\n!.gitignore')


def download_one(entry):
    # ... (код без изменений) ...
    url = entry['url']
    desc = entry['desc']
    temp_path = DATA_DIR / ("tmp_" + os.urandom(4).hex())
    result = {'entry': entry, 'error': None}
    try:
        print(f"Начинаю загрузку: {desc} ({url})")
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(temp_path, 'wb') as f:
                for chunk in r.iter_content(CHUNK_SIZE):
                    f.write(chunk)
        size_bytes = temp_path.stat().st_size
        size_mb = round(size_bytes / (1024 * 1024), 2)
        if size_bytes == 0: raise ValueError("Файл пустой.")
        if size_bytes > MAX_FILE_SIZE_MB * 1024 * 1024: raise ValueError(f"Файл слишком большой ({size_mb} MB > {MAX_FILE_SIZE_MB} MB).")
        result.update({'size_mb': size_mb, 'temp_path': temp_path})
        return result
    except Exception as e:
        result['error'] = f"Ошибка загрузки: {e}"
        print(f"Ошибка для {desc}: {result['error']}")
        if temp_path.exists(): temp_path.unlink()
    return result


def download_icon(session, url, save_path):
    """Скачивает одну иконку, используя общую сессию."""
    try:
        with session.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        return True
    except requests.RequestException as e:
        # Не выводим ошибку для каждой иконки, чтобы не засорять лог
        # print(f"Не удалось скачать иконку {url}: {e}", file=sys.stderr)
        return False

def _parse_icon_source_file(file_path, desc):
    """Парсит один EPG файл и возвращает список найденных иконок. Функция для распараллеливания."""
    print(f"Сканирую источник иконок: {desc}")
    found_icons = []
    try:
        open_func = gzip.open if is_gzipped(file_path) else open
        with open_func(file_path, 'rb') as f:
            tree = etree.parse(f)
        root = tree.getroot()

        for channel in root.findall('channel'):
            channel_id = channel.get('id')
            icon_tag = channel.find('icon')
            if channel_id and icon_tag is not None and 'src' in icon_tag.attrib:
                icon_url = icon_tag.get('src')
                names = get_channel_names(channel)
                if names and icon_url:
                    found_icons.append((desc, channel_id, names, icon_url))
    except (etree.XMLSyntaxError, gzip.BadGzipFile, ValueError) as e:
        print(f"Ошибка парсинга {file_path.name} для сбора иконок: {e}", file=sys.stderr)
    return found_icons

def build_icon_database(download_results):
    """Сканирует EPG-источники, скачивает иконки и создает базу. Оптимизировано."""
    print("\n--- Этап 1: Создание базы данных иконок (параллельный парсинг) ---")
    icon_db = {}
    icon_urls_to_download = {}
    
    # Распараллеливаем парсинг XML-файлов
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for result in download_results:
            if not result.get('error') and result['entry']['ico_src']:
                futures.append(executor.submit(_parse_icon_source_file, result['temp_path'], result['entry']['desc']))

        for future in as_completed(futures):
            for desc, channel_id, names, icon_url in future.result():
                parsed_url = urlparse(icon_url)
                filename = Path(parsed_url.path).name or f"{channel_id}.png"
                local_icon_path = ICONS_DIR / filename
                db_key = f"{desc}_{channel_id}"
                icon_db[db_key] = {'icon_path': local_icon_path, 'names': names}
                # Собираем уникальные URL для скачивания
                icon_urls_to_download[icon_url] = local_icon_path
    
    print(f"Найдено {len(icon_db)} каналов с иконками в источниках.")
    print(f"Требуется скачать {len(icon_urls_to_download)} уникальных иконок.")
    
    # Параллельная загрузка иконок с использованием одной сессии
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Используем partial, чтобы передать сессию в каждый вызов
            downloader = partial(download_icon, session)
            future_to_url = {executor.submit(downloader, url, path): url for url, path in icon_urls_to_download.items()}
            
            # Можно добавить прогресс-бар, но в CI это лишнее
            for future in as_completed(future_to_url):
                future.result()

    print("Загрузка иконок завершена.")
    return icon_db


def find_best_match(channel_names, icon_db):
    """Находит наиболее подходящую иконку, используя быструю библиотеку thefuzz."""
    if not channel_names: return None
        
    best_match_score = 0
    best_match_path = None

    for db_entry in icon_db.values():
        db_names = db_entry['names']
        if not db_names: continue
        
        if channel_names & db_names:
            return db_entry['icon_path']

        # Используем fuzz.token_set_ratio, он хорошо работает с разным порядком слов и лишними словами
        score = fuzz.token_set_ratio(' '.join(sorted(list(channel_names))), ' '.join(sorted(list(db_names))))
        
        if score > best_match_score:
            best_match_score = score
            best_match_path = db_entry['icon_path']
            
    if best_match_score >= SIMILARITY_THRESHOLD:
        return best_match_path
        
    return None

def process_epg_file(file_path, icon_db, owner, repo_name):
    # ... (код без изменений) ...
    print(f"Обрабатываю файл: {file_path.name}")
    try:
        was_gzipped = is_gzipped(file_path)
        open_func = gzip.open if was_gzipped else open
        parser = etree.XMLParser(remove_blank_text=True)
        with open_func(file_path, 'rb') as f:
            tree = etree.parse(f, parser)
        root = tree.getroot()
        changes_made = 0
        for channel in root.findall('channel'):
            channel_names = get_channel_names(channel)
            matched_icon_path = find_best_match(channel_names, icon_db)
            if matched_icon_path:
                new_icon_url = RAW_BASE_URL.format(owner=owner, repo=repo_name, filepath=matched_icon_path.as_posix())
                icon_tag = channel.find('icon')
                if icon_tag is None:
                    icon_tag = etree.SubElement(channel, 'icon')
                if icon_tag.get('src') != new_icon_url:
                    icon_tag.set('src', new_icon_url)
                    changes_made += 1
        if changes_made > 0:
            print(f"Внесено {changes_made} изменений в иконки файла {file_path.name}.")
            doctype_str = '<!DOCTYPE tv SYSTEM "https://iptvx.one/xmltv.dtd">'
            if was_gzipped:
                with gzip.open(file_path, 'wb') as f_out:
                    f_out.write(etree.tostring(tree, pretty_print=True, xml_declaration=True, encoding='UTF-8', doctype=doctype_str))
            else:
                tree.write(str(file_path), pretty_print=True, xml_declaration=True, encoding='UTF-8', doctype=doctype_str)
        return True
    except Exception as e:
        print(f"Критическая ошибка при обработке {file_path}: {e}", file=sys.stderr)
        return False

def shorten_url_safely(url):
    # ... (код без изменений) ...
    try:
        shortener = gdshortener.ISGDShortener()
        short_tuple = shortener.shorten(url)
        return short_tuple[0] if short_tuple and short_tuple[0] else "не удалось сократить"
    except Exception as e:
        # print(f"Не удалось сократить URL {url}: {e}", file=sys.stderr)
        return "не удалось сократить"

def update_readme(results, notes):
    # ... (код без изменений) ...
    utc_now = datetime.now(timezone.utc)
    timestamp = utc_now.strftime('%Y-%m-%d %H:%M %Z')
    lines = []
    if notes: lines.extend([notes, "\n---"])
    lines.append(f"\n# Обновлено: {timestamp}\n")
    for idx, r in enumerate(results, 1):
        lines.append(f"### {idx}. {r['entry']['desc']}")
        lines.append("")
        if r.get('error'):
            lines.extend([f"**Статус:** 🔴 Ошибка", f"**Источник:** `{r['entry']['url']}`", f"**Причина:** {r.get('error')}"])
        else:
            lines.extend([f"**Размер:** {r['size_mb']} MB", "", f"**Основная ссылка (GitHub Raw):**", f"`{r['raw_url']}`", "", "> **Альтернативные ссылки:**", ">", f"> - *Короткая (некоторые плееры не поддерживают):* `{r['short_raw_url']}`"])
            if r.get('jsdelivr_url'):
                lines.append(f"> - *CDN (jsDelivr):* `{r['jsdelivr_url']}` (Короткая (некоторые плееры не поддерживают): `{r['short_jsdelivr_url']}`)")
        lines.append("\n---")
    with open(README_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
    print(f"README.md обновлён ({len(results)} записей)")


def main():
    repo = os.getenv('GITHUB_REPOSITORY')
    if not repo or '/' not in repo:
        print("Ошибка: не удалось определить GITHUB_REPOSITORY.", file=sys.stderr)
        sys.exit(1)
    
    owner, repo_name = repo.split('/')
    
    sources, notes = read_sources_and_notes()
    clear_dirs()

    # --- Этап 0: Загрузка EPG ---
    print("\n--- Этап 0: Загрузка EPG файлов ---")
    download_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_entry = {executor.submit(download_one, entry): entry for entry in sources}
        for future in as_completed(future_to_entry):
            download_results.append(future.result())

    # --- Этап 1: Создание базы иконок ---
    icon_db = build_icon_database(download_results)
    
    # --- Этап 2: Обработка EPG файлов ---
    print("\n--- Этап 2: Замена ссылок на иконки в EPG файлах ---")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for res in download_results:
            if not res.get('error'):
                futures.append(executor.submit(process_epg_file, res['temp_path'], icon_db, owner, repo_name))
        for future in as_completed(futures):
            future.result()
    
    # --- Этап 3: Финализация и создание README ---
    # ... (код без изменений) ...
    print("\n--- Этап 3: Формирование финальных ссылок и README.md ---")
    url_to_result = {res['entry']['url']: res for res in download_results}
    ordered_results = [url_to_result[s['url']] for s in sources]
    final_results = []
    used_names = set()
    for res in ordered_results:
        if res.get('error'):
            final_results.append(res)
            continue
        true_extension = '.xml.gz' if is_gzipped(res['temp_path']) else '.xml'
        filename_from_url = Path(urlparse(res['entry']['url']).path).name or "download"
        base_name = filename_from_url.split('.')[0]
        proposed_filename = f"{base_name}{true_extension}"
        final_name = proposed_filename
        counter = 1
        while final_name in used_names:
            p = Path(proposed_filename)
            stem = p.name.replace(''.join(p.suffixes), '')
            final_name = f"{stem}-{counter}{''.join(p.suffixes)}"
            counter += 1
        used_names.add(final_name)
        target_path = DATA_DIR / final_name
        res['temp_path'].rename(target_path)
        raw_url = RAW_BASE_URL.format(owner=owner, repo=repo_name, filepath=target_path.as_posix())
        res['raw_url'] = raw_url
        res['short_raw_url'] = shorten_url_safely(raw_url)
        if res['size_mb'] < JSDELIVR_SIZE_LIMIT_MB:
            jsdelivr_url = JSDELIVR_BASE_URL.format(owner=owner, repo=repo_name, filepath=target_path.as_posix())
            res['jsdelivr_url'] = shorten_url_safely(jsdelivr_url)
        final_results.append(res)

    update_readme(final_results, notes)
    print("\nСкрипт успешно завершил работу.")


if __name__ == '__main__':
    main()
