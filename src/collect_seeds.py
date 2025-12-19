import asyncio
import csv
import os

import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests

from . import logger, FILENAME_FOR_CANDIDATES, HEADERS, PROJECT_ROOT, RAW_DIR

load_dotenv(PROJECT_ROOT / '.env')

FORUM_URL = 'https://auditorium-cg.ru/contact-center25'
CCWF_URL = 'https://ccwf.ru/speakers'
DADATA_API_KEY = os.getenv('DADATA_API_KEY')
DADATA_API_URL = (
    'https://suggestions.dadata.ru/'
    'suggestions/api/4_1/rs/suggest/party'
)
DADATA_SEMAPHORE = asyncio.Semaphore(10)


def parse_participants_from_forum(html_content):
    """Парсит список компаний-участников форума контакт центр 2025."""
    companies = []
    title_tag = BeautifulSoup(html_content, 'html.parser').find(
        'h2', class_='content__parttitle',
        string=lambda t: 'Участники Форума' in t if t else False
    )
    strong_tags = title_tag.find_all_next('strong')
    index_strong = 0
    while index_strong < len(strong_tags):
        current_strong = strong_tags[index_strong]
        company_name = current_strong.get_text(strip=True)
        index_next_strong = index_strong + 1
        while index_next_strong < len(strong_tags):
            next_strong = strong_tags[index_next_strong]
            is_adjacent = False
            next_sibling = current_strong.find_next_sibling()
            while next_sibling:
                if next_sibling == next_strong:
                    is_adjacent = True
                    break
                elif next_sibling.name:
                    break
                next_sibling = next_sibling.find_next_sibling()
            if not is_adjacent:
                break
            company_name += next_strong.get_text(strip=True)
            current_strong = next_strong
            index_next_strong += 1
        next_element = current_strong.find_next_sibling()
        while next_element and not next_element.name:
            next_element = next_element.find_next_sibling()
        if company_name and len(company_name) >= 2:
            companies.append({'name': company_name})
        index_strong = index_next_strong
    return companies


def parse_speakers_from_ccwf(html_content):
    """Парсит список компаний-спикеров ccwf."""
    companies = []
    for card in BeautifulSoup(
        html_content, 'html.parser'
    ).find_all('div', class_='t537__itemwrapper'):
        company_div = card.find('div', class_='t537__perstext')
        if company_div:
            company_name = company_div.get_text(strip=True)
            if company_name and len(company_name) >= 2:
                companies.append({'name': company_name})
    return companies


async def check_company_dadata(session, company_name):
    """Асинхронная проверка компании."""
    async with DADATA_SEMAPHORE:
        try:
            async with session.post(
                DADATA_API_URL,
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'Authorization': f'Token {DADATA_API_KEY}'
                },
                json={'query': company_name, 'count': 1}
            ) as response:
                if response.status != 200:
                    return None
                data = await response.json()
                if not data.get('suggestions'):
                    return None
                company_data = data['suggestions'][0].get('data', {})
                inn = str(company_data.get('inn', ''))
                if len(inn) != 10 or not inn.isdigit():
                    return None
                country = company_data.get(
                    'address', {}
                ).get('data', {}).get('country', '')
                if not country or country.lower() not in (
                    'россия', 'russia', 'ru'
                ):
                    return None
                return {'name': company_name, 'inn': inn}
        except Exception as e:
            logger.error(f'Ошибка для {company_name}: {type(e).__name__}')
    return None


async def process_companies_async(company_names):
    async with aiohttp.ClientSession() as session:
        tasks = [
            check_company_dadata(session, name)
            for name in company_names
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    return [result for result in results if isinstance(result, dict)]


def normalize_company_name(name):
    """Приводит название компании к стандартному виду для сравнения."""
    if not name:
        return ''
    normalized = name.strip().lower()
    forms_to_remove = [
        'ооо ', 'зао ', 'ао ', 'пао ', 'оао ', 'ип ', 'нко ', 'мкк ',
        'общество с ограниченной ответственностью ',
        'акционерное общество ', 'публичное акционерное общество '
    ]
    for form in forms_to_remove:
        if normalized.startswith(form):
            normalized = normalized[len(form):]
    normalized = normalized.replace('"', '').replace("'", "")
    normalized = ' '.join(normalized.split())
    if normalized.endswith(' банк'):
        normalized = 'банк ' + normalized[:-5]
    return normalized


def save_to_csv(companies, filename=FILENAME_FOR_CANDIDATES):
    """Сохраняет компании в CSV, убирая дубликаты по ИНН"""
    if not companies:
        logger.warning('Нет компаний для сохранения')
        return False
    unique_by_inn = {}
    for company in companies:
        inn = company.get('inn')
        if inn:
            if inn not in unique_by_inn:
                unique_by_inn[inn] = company
    unique_companies = list(unique_by_inn.values())
    try:
        with open(
            RAW_DIR / filename, 'w', newline='', encoding='utf-8-sig'
        ) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=('name', 'inn'))
            writer.writeheader()
            writer.writerows(unique_companies)
        return True
    except Exception as e:
        logger.error(f'Ошибка сохранения: {type(e).__name__}: {e}')
        return False


async def main_async():
    if not DADATA_API_KEY:
        logger.error('Отсутствует API ключ DaData.')
        return
    try:
        companies = []
        for site, func in (
            (FORUM_URL, parse_participants_from_forum),
            (CCWF_URL, parse_speakers_from_ccwf)
        ):
            response = requests.get(site, headers={
                **HEADERS,
                'Accept': (
                    'text/html,application/xhtml+xml'
                    ',application/xml;q=0.9,*/*;q=0.8'
                )
            })
            response.raise_for_status()
            companies.extend(func(response.text))
        result = {}
        for company in companies:
            if norm_name := normalize_company_name(company['name']):
                if norm_name not in result or len(
                    company['name']
                ) > len(result[norm_name]):
                    result[norm_name] = company['name']
        valid_companies = await process_companies_async(
            [company['name'] for company in [
                {'name': orig_name} for _, orig_name in result.items()
            ]]
        )
        if valid_companies:
            save_to_csv(valid_companies)
    except requests.RequestException as e:
        logger.error(f'Ошибка при загрузке страниц: {str(e)}')
    except Exception as e:
        logger.error(f'Общая ошибка: {type(e).__name__}: {str(e)}')


def main():
    asyncio.run(main_async())
