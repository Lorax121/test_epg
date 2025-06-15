import os
import sys
import json
import requests
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import gdshortener
import gzip
import re
from difflib import SequenceMatcher

try:
    from lxml import etree
except ImportError:
    print("Ошибка: библиотека lxml не установлена. Пожалуйста, установите ее: pip install lxml", file=sys.stderr)
    sys.exit(1)

SOURCES_FILE = 'sources.json'
DATA_DIR = Path('data')
ICONS_DIR = Path('icons') 
README_FILE = 'README.md'
MAX_WORKERS = 10
CHUNK_SIZE = 16 * 1024
MAX_FILE_SIZE_MB = 95
JSDELIVR_SIZE_LIMIT_MB = 20
SIMILARITY_THRESHOLD = 0.8 

RAW_BASE_URL = "https://raw.githubusercontent.com/{owner}/{repo}/main/{filepath}"
JSDELIVR_BASE_URL = "https://cdn.jsdelivr.net/gh/{owner}/{repo}@main/{filepath}"

def is_gzipped(file_path):
    """Проверяет, является ли файл gzipped, по его магическим байтам."""
    with open(file_path, 'rb') as f:
        return f.read(2) == b'\x1f\x8b'

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



def read_sources_and_notes():
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
    """Очищает папки data и icons."""
    for d in [DATA_DIR, ICONS_DIR]:
        if d.exists():
            for f in d.iterdir():
                if f.is_file():
                    f.unlink()
        else:
            d.mkdir(parents=True, exist_ok=True)
    gitignore = ICONS_DIR / '.gitignore'
    if not gitignore.exists():
        gitignore.write_text('*\n!.gitignore')


def download_one(entry):
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
        if size_bytes == 0:
            raise ValueError("Файл пустой.")
        if size_bytes > MAX_FILE_SIZE_MB * 1024 * 1024:
            raise ValueError(f"Файл слишком большой ({size_mb} MB > {MAX_FILE_SIZE_MB} MB).")
        
        result.update({
            'size_mb': size_mb,
            'temp_path': temp_path
        })
        return result
    except Exception as e:
        result['error'] = f"Ошибка загрузки: {e}"
        print(f"Ошибка для {desc}: {result['error']}")
        if temp_path.exists():
            temp_path.unlink()
    return result


def download_icon(url, save_path):
    """Скачивает одну иконку."""
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        return True
    except requests.RequestException as e:
        print(f"Не удалось скачать иконку {url}: {e}", file=sys.stderr)
        return False

def build_icon_database(download_results):
    """Сканирует EPG-источники, помеченные как ico_src, скачивает иконки и создает базу."""
    print("\n--- Этап 1: Создание базы данных иконок ---")
    icon_db = {}
    icon_urls_to_download = {}

    for result in download_results:
        if result.get('error') or not result['entry']['ico_src']:
            continue
        
        print(f"Сканирую источник иконок: {result['entry']['desc']}")
        file_path = result['temp_path']
        
        try:
            # <<< ИЗМЕНЕНИЕ: Используем функцию is_gzipped для определения способа открытия >>>
            open_func = gzip.open if is_gzipped(file_path) else open
            with open_func(file_path, 'rb') as f:
                # lxml.etree.parse может принимать файловый объект
                tree = etree.parse(f)
            root = tree.getroot()

            for channel in root.findall('channel'):
                channel_id = channel.get('id')
                icon_tag = channel.find('icon')
                if channel_id and icon_tag is not None and 'src' in icon_tag.attrib:
                    icon_url = icon_tag.get('src')
                    names = get_channel_names(channel)
                    if not names or not icon_url:
                        continue

                    parsed_url = urlparse(icon_url)
                    filename = Path(parsed_url.path).name or f"{channel_id}.png"
                    
                    local_icon_path = ICONS_DIR / filename
                    
                    db_key = f"{result['entry']['desc']}_{channel_id}"
                    icon_db[db_key] = {'icon_path': local_icon_path, 'names': names}
                    
                    if not local_icon_path.exists():
                        icon_urls_to_download[icon_url] = local_icon_path

        except (etree.XMLSyntaxError, gzip.BadGzipFile, ValueError) as e:
            # <<< ИЗМЕНЕНИЕ: Добавили имя файла в сообщение об ошибке для ясности >>>
            print(f"Ошибка парсинга {file_path.name} для сбора иконок: {e}", file=sys.stderr)

    print(f"Найдено {len(icon_db)} каналов с иконками в источниках.")
    print(f"Требуется скачать {len(icon_urls_to_download)} уникальных иконок.")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS * 2) as executor:
        future_to_url = {executor.submit(download_icon, url, path): url for url, path in icon_urls_to_download.items()}
        for future in as_completed(future_to_url):
            future.result() 

    print("Загрузка иконок завершена.")
    return icon_db


def find_best_match(channel_names, icon_db):
    """Находит наиболее подходящую иконку в базе данных."""
    if not channel_names:
        return None
        
    best_match_score = 0
    best_match_path = None

    for db_entry in icon_db.values():
        db_names = db_entry['names']
        if not db_names:
            continue
        
        if channel_names & db_names:
            return db_entry['icon_path']

        current_max_score = 0
        for name1 in channel_names:
            for name2 in db_names:
                score = SequenceMatcher(None, name1, name2).ratio()
                if score > current_max_score:
                    current_max_score = score
        
        if current_max_score > best_match_score:
            best_match_score = current_max_score
            best_match_path = db_entry['icon_path']
            
    if best_match_score >= SIMILARITY_THRESHOLD:
        return best_match_path
        
    return None

def process_epg_file(file_path, icon_db, owner, repo_name):
    """Обрабатывает один EPG файл: находит и заменяет URL иконок."""
    print(f"Обрабатываю файл: {file_path.name}")
    try:
        # <<< ИЗМЕНЕНИЕ: Запоминаем, был ли файл сжат изначально >>>
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
                # Используем относительный путь для ссылок
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
            
            # <<< ИЗМЕНЕНИЕ: Логика сохранения теперь тоже использует was_gzipped >>>
            if was_gzipped:
                # Сразу пишем в сжатый файл
                with gzip.open(file_path, 'wb') as f_out:
                    f_out.write(etree.tostring(tree, pretty_print=True, xml_declaration=True, encoding='UTF-8', doctype=doctype_str))
            else:
                # Пишем в обычный файл
                tree.write(str(file_path), pretty_print=True, xml_declaration=True, encoding='UTF-8', doctype=doctype_str)

        return True

    except Exception as e:
        print(f"Критическая ошибка при обработке {file_path}: {e}", file=sys.stderr)
        return False


def shorten_url_safely(url):
    try:
        shortener = gdshortener.ISGDShortener()
        short_tuple = shortener.shorten(url)
        return short_tuple[0] if short_tuple and short_tuple[0] else "не удалось сократить"
    except Exception as e:
        print(f"Не удалось сократить URL {url}: {e}", file=sys.stderr)
        return "не удалось сократить"


def update_readme(results, notes):
    utc_now = datetime.now(timezone.utc)
    timestamp = utc_now.strftime('%Y-%m-%d %H:%M %Z')
    
    lines = []

    if notes:
        lines.append(notes)
        lines.append("\n---")
    
    lines.append(f"\n# Обновлено: {timestamp}\n")

    for idx, r in enumerate(results, 1):
        lines.append(f"### {idx}. {r['entry']['desc']}")
        lines.append("")
        if r.get('error'):
            lines.append(f"**Статус:** 🔴 Ошибка")
            lines.append(f"**Источник:** `{r['entry']['url']}`")
            lines.append(f"**Причина:** {r.get('error')}")
        else:
            lines.append(f"**Размер:** {r['size_mb']} MB")
            lines.append("")
            
            lines.append(f"**Основная ссылка (GitHub Raw):**")
            lines.append(f"`{r['raw_url']}`")
            lines.append("")

            lines.append("> **Альтернативные ссылки:**")
            lines.append(">") 
            lines.append(f"> - *Короткая (некоторые плееры не поддерживают):* `{r['short_raw_url']}`")
            
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

    print("\n--- Этап 0: Загрузка EPG файлов ---")
    download_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_entry = {executor.submit(download_one, entry): entry for entry in sources}
        for future in as_completed(future_to_entry):
            download_results.append(future.result())

    icon_db = build_icon_database(download_results)
    
    print("\n--- Этап 2: Замена ссылок на иконки в EPG файлах ---")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for res in download_results:
            if not res.get('error'):
                futures.append(executor.submit(process_epg_file, res['temp_path'], icon_db, owner, repo_name))
        for future in as_completed(futures):
            future.result() 
    
    # --- Этап 3: Финализация и создание README ---
    print("\n--- Этап 3: Формирование финальных ссылок и README.md ---")
    
    url_to_result = {res['entry']['url']: res for res in download_results}
    ordered_results = [url_to_result[s['url']] for s in sources]

    final_results = []
    used_names = set()
    for res in ordered_results:
        if res.get('error'):
            final_results.append(res)
            continue
        
        # <<< ИЗМЕНЕНИЕ: Возвращаем надежное определение расширения файла >>>
        if is_gzipped(res['temp_path']):
            true_extension = '.xml.gz'
        else:
            # Дополнительная проверка на XML на всякий случай
            try:
                with open(res['temp_path'], 'rb') as f:
                    sig = f.read(5)
                if sig.startswith(b'<?xml'):
                    true_extension = '.xml'
                else:
                    # Если не gzip и не xml, берем из URL
                    true_extension = ''.join(Path(urlparse(res['entry']['url']).path).suffixes) or '.xml'
            except Exception:
                true_extension = '.xml'
             
        filename_from_url = Path(urlparse(res['entry']['url']).path).name or "download"
        base_name = filename_from_url.split('.')[0]
        # Используем .suffixes для случаев типа file.xml.gz
        proposed_filename = f"{base_name}{true_extension}"

        final_name = proposed_filename
        counter = 1
        while final_name in used_names:
            p = Path(proposed_filename)
            # Правим формирование имени для случаев с двойным расширением
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
            res['jsdelivr_url'] = jsdelivr_url
            res['short_jsdelivr_url'] = shorten_url_safely(jsdelivr_url)
        
        final_results.append(res)

    update_readme(final_results, notes)
    print("\nСкрипт успешно завершил работу.")

if __name__ == '__main__':
    main()
