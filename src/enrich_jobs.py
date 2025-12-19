import asyncio
import re
from dataclasses import dataclass, field
from typing import List

import aiohttp
import pandas as pd

from . import (
    logger,
    FILENAME_FOR_CANDIDATES,
    FILENAME_FOR_PARSE_JOBS,
    HEADERS,
    RAW_DIR
)


@dataclass
class ValidCompanyResult:
    """Валидный результат компании."""
    name: str
    inn: str
    site: str
    support_team_size_min: int
    support_evidence: str
    evidence_url: str
    evidence_type: str
    source: str = 'jobs_analysis'
    hh_employer_id: str = ''
    support_vacancies_count: int = 0
    support_vacancy_titles: List[str] = field(default_factory=list)
    vacancies_sample_urls: List[str] = field(default_factory=list)
    has_support_section: bool = False
    has_support_email: bool = False
    has_contact_form: bool = False
    has_online_chat: bool = False
    has_messengers: bool = False
    has_kb_or_faq: bool = False
    mentions_24_7: bool = False
    shift_work_mentioned: bool = False
    support_email: str = ''
    support_url: str = ''
    kb_url: str = ''
    job_titles_found: str = ''
    jobs_url: str = ''
    estimated_team_from_jobs: int = 0
    jobs_evidence: str = ''
    error: str = ''
    parsed_successfully: bool = True

    def to_dict(self):
        """Конвертирует в словарь для сохранения."""
        return {
            'name': self.name,
            'inn': self.inn,
            'site': self.site,
            'support_team_size_min': self.support_team_size_min,
            'support_evidence': self.support_evidence,
            'evidence_url': self.evidence_url,
            'evidence_type': self.evidence_type,
            'source': self.source,
            'hh_employer_id': self.hh_employer_id,
            'support_vacancies_count': self.support_vacancies_count,
            'support_vacancy_titles': ' | '.join(
                self.support_vacancy_titles[:10]
            ) if self.support_vacancy_titles else '',
            'vacancies_sample_urls': ' | '.join(
                self.vacancies_sample_urls[:3]
            ) if self.vacancies_sample_urls else '',
            'has_support_section': str(self.has_support_section).lower(),
            'has_support_email': str(self.has_support_email).lower(),
            'has_contact_form': str(self.has_contact_form).lower(),
            'has_online_chat': str(self.has_online_chat).lower(),
            'has_messengers': str(self.has_messengers).lower(),
            'has_kb_or_faq': str(self.has_kb_or_faq).lower(),
            'mentions_24_7': str(self.mentions_24_7).lower(),
            'shift_work_mentioned': str(self.shift_work_mentioned).lower(),
            'support_email': self.support_email,
            'support_url': self.support_url,
            'kb_url': self.kb_url,
            'job_titles_found': self.job_titles_found,
            'jobs_url': self.jobs_url,
            'estimated_team_from_jobs': self.estimated_team_from_jobs,
            'jobs_evidence': self.jobs_evidence,
            'error': self.error,
            'parsed_successfully': str(self.parsed_successfully).lower(),
            'is_valid': True
        }


class HHSupportAnalyzer:
    """Анализатор поддержки через HeadHunter API и анализ сайтов"""

    def __init__(self, max_concurrent: int = 3, request_timeout: float = 15):
        self.request_timeout = request_timeout
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.support_keywords = (
            r'поддержк[а-яё]*', r'helpdesk', r'service\s*desk',
            r'оператор', r'консультант', r'специалист\s+поддержки',
            r'менеджер\s+(?:по\s+)?работе\s+с\s+клиентами', r'график\s+2/2',
            r'customer\s*support', r'клиентск[а-яё]*\s+поддержк[а-яё]*',
            r'контакт[-\s]*центр', r'call[\s-]*центр', r'колл[\s-]*центр',
            r'L[123]\s*(?:поддержк[а-яё]*|специалист)', r'агент\s+поддержки',
            r'support\s+agent', r'саппорт', r'обслуживани[ея]\s+клиент[ов]*',
            r'диспетчер', r'приём\s+звонков', r'входящ[а-яё]+\s+лини[яи]',
            r'техническ[а-яё]+\s+поддержк[а-яё]*', r'техподдержка',
            r'IT[\s-]*поддержк[а-яё]*', r'инженер\s+поддержки',
            r'супервайзер', r'тимлид.*поддержк', r'руководитель.*поддержк',
            r'старший.*оператор', r'ведущий.*специалист.*поддержк',
            r'чат[\s-]*поддержк[а-яё]*', r'письменн[а-яё]+\s+поддержк[а-яё]*',
            r'email[\s-]*поддержк[а-я]*', r'модератор', r'3\s*смен[ы]',
            r'(?:без|не)\s+продаж', r'не+продажи', r'обслуживание\s+клиентов',
            r'сменн[а-яё]+\s+работ[аы]', r'ночн[а-яё]+\s+смен[аы]',
            r'тех[.\-]*поддержк[а-я]*', r'посменн[а-яё]+\s+работ[аы]'
        )
        self.exclude_patterns = (
            (
                r'менеджер\s+по\s+продажам',
                lambda text: 'без продаж' not in text.lower()
            ),
            (
                r'(?:разработчик|программист|devops|бэкэнд|фронтэнд)',
                lambda text: True
            ),
            (
                r'(?:маркетолог|бухгалтер|юрист|дизайнер|аналитик|архитектор)',
                lambda text: True
            ),
            (r'менеджер\s+проект', lambda text: True),
        )
        self.team_size_patterns = (
            (
                r'(?:в\s+)?(?:служб[еа]|отдел[ее]|команд[ае])\s+поддержк[а-яё]'
                r'*[\s\w,.-]{0,60}?(\d{2,})\s*(?:человек|сотрудник|специалист'
                r'|оператор|агентов?|менеджеров?|мест)'
            ),
            (
                r'контакт[-\s]*центр[\s\w,.-]{0,60}?(\d{2,})\s*(?:человек|сот'
                r'рудник|оператор|мест|работников|сотруднико?в)'
            ),
            (
                r'(?:более|около|свыше|до)\s+(\d{2,})\s*(?:человек|'
                r'сотрудник)[\s\w,.-]{0,40}(?:в\s+)?поддержк'
            ),
            (
                r'(\d{2,})\s*(?:человек|сотрудник|специалист)[\s\w,.-]{0,30}(?'
                r'работа[еюю]т|занят[оы]?|в\s+)(?:поддержк|контакт[-\s]*центр)'
            ),
            (
                r'поддержк[а-яё]*[\s\w,.-]{0,30}?из'
                r'\s+(\d{2,})\s*(?:человек|сотрудник)'
            ),
            (
                r'штат\s+(?:поддержк|контакт[-\s]*центр)[\s\w,.-]{0,30}'
                r'?(\d{2,})\s*(?:человек|сотрудник)'
            ),
            (
                r'(?:в\s+)?нашей\s+(?:команд[ее]|поддержк)[\s\w,.-]{0,50}'
                r'?(\d{2,})\s*(?:человек|сотрудник)'
            ),
            (
                r'(?:работать?|работа[еюю]м)\s+в\s+(?:команд[ее]|отдел[ее])'
                r'[\s\w,.-]{0,50}?(\d{2,})\s*(?:человек|сотрудник)'
            ),
            (
                r'размер[а-яё]*\s+(?:команд[ыы]|отдел[аа])[\s\w,.-]'
                r'{0,40}?(\d{2,})\s*(?:человек|сотрудник)'
            )
        )
        self.shift_patterns = (
            r'24[/×x]7', r'24\s*часа', r'круглосуточно', r'круглые\s+сутки',
            r'работаем\s+(?:всегда|постоянно)', r'без\s+выходных',
            r'сменн[а-яё]+\s+работ[аы]', r'график\s+2/2', r'3\s*смен[ы]',
            r'посменн[а-яё]+\s+работ[аы]', r'ночн[а-яё]+\s+смен[аы]',
            r'дежурств[а-яё]*\s+по\s+графику', r'скользящ[а-яё]+\s+график'
        )
        self.load_patterns = (
            r'тысяч[а-яё]*\s+(?:обращен|звонк|заявк)',
            r'ежедневн[а-яё]+\s+(?:обрабатываем|принимаем)',
            r'объем[а-яё]*\s+(?:обращен|звонков)',
            r'(?:больш[а-яё]+|крупн[а-яё]+)\s+(?:нагрузк|поток)',
            r'много\s+(?:обращен|звонков|клиентов)',
            r'высок[а-яё]+\s+(?:нагрузк|интенсивность)',
        )
        self.role_categories = {
            'оператор': ('оператор', 'диспетчер', 'приём звонков'),
            'менеджер': ('менеджер', 'супервайзер', 'руководитель'),
            'техподдержка': ('техническ', 'инженер', 'IT', 'L2', 'L3'),
            'чат_поддержка': ('чат', 'письменн', 'email', 'модератор'),
            'консультант': ('консультант', 'специалист', 'агент'),
        }
        self.employer_cache = {}
        self.vacancies_cache = {}
        self.website_cache = {}

    async def __aenter__(self):
        """Инициализация сессии."""
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(force_close=True),
            timeout=aiohttp.ClientTimeout(total=self.request_timeout),
            headers={
                **HEADERS,
                'Accept': 'application/json, text/html'
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие сессии."""
        if self.session:
            await self.session.close()
            self.session = None
        return False

    def support_vacancy(self, title, snippet=''):
        """Определяет, является ли вакансия вакансией поддержки."""
        if not title or len(title) < 3:
            return False, ''
        title_lower = title.lower()
        full_text = f'{title_lower} {(snippet or '').lower()}'
        for pattern, condition in self.exclude_patterns:
            if re.search(pattern, title_lower, re.IGNORECASE):
                if condition(full_text):
                    return False, 'excluded_role'
        has_support_keyword = False
        for pattern in self.support_keywords:
            if re.search(pattern, title_lower, re.IGNORECASE):
                has_support_keyword = True
                break
        if not has_support_keyword:
            return False, 'no_support_keyword'
        if 'менеджер' in title_lower:
            if 'продаж' in title_lower and 'без продаж' not in full_text:
                return False, 'sales_manager'
            if 'клиент' not in full_text and 'поддерж' not in full_text:
                return False, 'non_client_manager'
        return True, 'support_vacancy'

    def analyze_vacancy_quality(self, title, snippet=''):
        """Анализирует качество вакансии для Уровня B."""
        text = f'{title.lower()} {snippet.lower()}'.lower()
        result = {
            'is_shift_work': any(re.search(
                pattern, text, re.IGNORECASE
            ) for pattern in self.shift_patterns),
            'has_load_mention': any(re.search(
                pattern, text, re.IGNORECASE
            ) for pattern in self.load_patterns),
            'is_24_7': any(
                re.search(pattern, text, re.IGNORECASE) for pattern in (
                    r'24[/×x]7', r'24\s*часа', r'круглосуточно'
                )
            ),
            'role_category': None,
            'is_l1_l2': bool(re.search(
                r'L[123]|(?:перва|втора|третья)\s+линия|1[-\s]?я\s+линия',
                text, re.IGNORECASE
            )),
            'is_chat_support': bool(re.search(
                r'чат|письменн|email|мессенджер', text, re.IGNORECASE
            )),
            'is_phone_support': bool(re.search(
                r'звонк|телефон|call|колл', text, re.IGNORECASE
            )),
        }
        for category, keywords in self.role_categories.items():
            for keyword in keywords:
                if keyword in text:
                    result['role_category'] = category
                    break
            if result['role_category']:
                break
        return result

    def analyze_vacancies_set(self, vacancies_data):
        """Анализирует набор вакансий с правильной классификацией."""
        if not vacancies_data:
            return {
                'score': 0,
                'team_size': 0,
                'evidence': 'Нет вакансий поддержки',
                'is_level_a': False,
                'shift_work': False,
                'mentions_24_7': False,
                'unique_roles': 0,
                'vacancy_count': 0,
                'details': {
                    'unique_roles': [],
                    'shift_work_count': 0,
                    'load_mention_count': 0,
                    'twentyfour_seven_count': 0,
                    'l1_l2_count': 0,
                    'has_chat_phone_both': False
                }
            }
        direct_size_evidence = None
        direct_context = ''
        direct_vacancy_url = ''
        for vac in vacancies_data:
            full_text = (
                f'{vac.get('title', '')} {vac.get('snippet', '')}'
            ).lower()
            direct_patterns = (
                (
                    r'(?:в\s+)?(?:нашей\s+)?(?:команд[еа]|отдел[еа]|'
                    r'служб[ае])\s+поддержк[а-яё]*[\s\w,.-]{0,50}?'
                    r'(\d{2,})\s*(?:человек|сотрудник|специалист|оператор)'
                ),
                (
                    r'работа[еюю]т\s+в\s+(?:поддержк|контакт[-\s]*центр)'
                    r'[\s\w,.-]{0,50}?(\d{2,})\s*(?:человек|сотрудник)'
                ),
                (
                    r'размер[\s\w,.-]{0,30}?(?:команд[ыы]|отдел[аа])'
                    r'[\s\w,.-]{0,30}?(\d{2,})\s*(?:человек|сотрудник)'
                ),
                (
                    r'(?:более|около|свыше|до)\s+(\d{2,})\s*(?:человек|'
                    r'сотрудник)[\s\w,.-]{0,40}(?:в\s+)?поддержк'
                ),
                (
                    r'поддержк[а-яё]*[\s\w,.-]{0,30}?из\s+(\d{2,})\s*'
                    r'(?:человек|сотрудник)'
                ),
                (
                    r'контакт[-\s]*центр[\s\w,.-]{0,60}?(\d{2,})\s*'
                    r'(?:человек|сотрудник|оператор|мест|работников)'
                ),
                (
                    r'штат\s+(?:поддержк|контакт[-\s]*центр)[\s\w,.-]'
                    r'{0,30}?(\d{2,})\s*(?:человек|сотрудник)'
                ),
                (
                    r'обрабатываем\s+в\s+день[\s\w,.-]{0,30}?'
                    r'(\d{2,})\s*(?:тысяч|запросов|обращений)'
                ),
                (
                    r'ежедневно\s+(?:обрабатываем|принимаем)[\s\w,.-]'
                    r'{0,30}?(\d{2,})\s*(?:тысяч|звонков)'
                ),
            )
            for pattern in direct_patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    try:
                        size = int(match.group(1))
                        if size >= 10:
                            context = full_text[
                                max(0, match.start() - 60):min(
                                    len(full_text), match.end() + 60
                                )
                            ].replace('\n', ' ').strip()
                            context = re.sub(r'\s+', ' ', context)
                            direct_size_evidence = size
                            direct_context = context
                            direct_vacancy_url = vac.get('url', '')
                            break
                    except (ValueError, IndexError):
                        continue
            if direct_size_evidence:
                break
        if direct_size_evidence:
            evidence = (
                'Уровень A: прямое упоминание размера команды '
                f'поддержки: "{direct_context}"'
            )
            if len(evidence) > 200:
                evidence = evidence[:197] + '...'
            return {
                'score': 100,
                'team_size': direct_size_evidence,
                'evidence': evidence,
                'is_level_a': True,
                'shift_work': False,
                'mentions_24_7': False,
                'unique_roles': 1,
                'vacancy_count': len(vacancies_data),
                'details': {
                    'unique_roles': ['direct_evidence'],
                    'shift_work_count': 0,
                    'load_mention_count': 0,
                    'twentyfour_seven_count': 0,
                    'l1_l2_count': 0,
                    'has_chat_phone_both': False,
                    'direct_evidence_size': direct_size_evidence,
                    'direct_evidence_context': direct_context,
                    'direct_evidence_url': direct_vacancy_url
                }
            }
        analysis_results = []
        unique_roles = set()
        shift_work_count = 0
        load_mention_count = 0
        twentyfour_seven_count = 0
        l1_l2_count = 0
        chat_phone_both = False
        for vac in vacancies_data:
            quality = self.analyze_vacancy_quality(
                vac.get('title', ''), vac.get('snippet', '')
            )
            analysis_results.append(quality)
            if quality['role_category']:
                unique_roles.add(quality['role_category'])
            if quality['is_shift_work']:
                shift_work_count += 1
            if quality['has_load_mention']:
                load_mention_count += 1
            if quality['is_24_7']:
                twentyfour_seven_count += 1
            if quality['is_l1_l2']:
                l1_l2_count += 1
        has_chat = any(r['is_chat_support'] for r in analysis_results)
        has_phone = any(r['is_phone_support'] for r in analysis_results)
        if has_chat and has_phone:
            chat_phone_both = True
        score = 0
        evidence_parts = []
        vacancy_count = len(vacancies_data)
        if vacancy_count >= 3:
            score += 20
            evidence_parts.append(f'{vacancy_count} вакансий поддержки')
        elif vacancy_count == 2:
            score += 10
            evidence_parts.append(f'{vacancy_count} вакансии поддержки')
        elif vacancy_count == 1:
            score += 5
            evidence_parts.append('1 вакансия поддержки')
        unique_role_count = len(unique_roles)
        if unique_role_count >= 2:
            score += 15
            evidence_parts.append(
                f'разные роли ({', '.join(list(unique_roles)[:3])})'
            )
        if twentyfour_seven_count > 0:
            score += 20
            evidence_parts.append('круглосуточная работа (24/7)')
        elif shift_work_count > 0:
            score += 15
            evidence_parts.append('сменная работа')
        if l1_l2_count > 0:
            score += 10
            evidence_parts.append('многоуровневая поддержка (L1/L2)')
        if chat_phone_both:
            score += 10
            evidence_parts.append('разные каналы поддержки (чат+телефон)')
        elif has_chat or has_phone:
            score += 5
        if load_mention_count > 0:
            score += 15
            evidence_parts.append('высокая нагрузка')
        if score >= 30:
            team_size = 18
            level_desc = 'несколько разных ролей и специализаций'
        elif score >= 20:
            team_size = 15
            level_desc = 'разные роли и каналы поддержки'
        elif score >= 15:
            team_size = 12
            level_desc = 'минимальная структура команды'
        elif score >= 10:
            team_size = 10
            level_desc = 'консервативная оценка по признакам ТЗ'
        else:
            team_size = 0
            level_desc = 'недостаточно доказательств для оценки ≥10'
        if evidence_parts:
            evidence = f'Уровень B: {", ".join(evidence_parts)}'
        elif vacancy_count > 0:
            evidence = f'Уровень B: {vacancy_count} вакансий поддержки'
        else:
            evidence = 'Уровень B: недостаточно доказательств'
        if team_size >= 10 and len(evidence_parts) > 0:
            evidence += f' ({level_desc})'
        return {
            'score': score,
            'team_size': team_size,
            'evidence': evidence,
            'is_level_a': False,
            'shift_work': shift_work_count > 0 or twentyfour_seven_count > 0,
            'mentions_24_7': twentyfour_seven_count > 0,
            'unique_roles': unique_role_count,
            'vacancy_count': vacancy_count,
            'details': {
                'unique_roles': list(unique_roles),
                'shift_work_count': shift_work_count,
                'load_mention_count': load_mention_count,
                'twentyfour_seven_count': twentyfour_seven_count,
                'l1_l2_count': l1_l2_count,
                'has_chat_phone_both': chat_phone_both,
                'direct_evidence_size': None,
                'direct_evidence_context': None,
                'direct_evidence_url': None
            }
        }

    async def find_employer_id(self, company_name):
        """Находит ID работодателя на HH."""
        cache_key = company_name.lower().strip()
        if cache_key in self.employer_cache:
            return self.employer_cache[cache_key]
        async with self.semaphore:
            try:
                async with self.session.get(
                    'https://api.hh.ru/employers',
                    params={
                        'text': f'{company_name}',
                        'area': '113',
                        'per_page': '20',
                        'only_with_vacancies': 'true'
                    }
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        items = data.get('items', [])
                        if items:
                            best_match = None
                            best_score = 0
                            search_name = company_name.lower()
                            search_words = set(search_name.split())
                            for item in items:
                                item_name = item.get('name', '').lower()
                                item_words = set(item_name.split())
                                score = 0
                                if search_name == item_name:
                                    score += 100
                                elif search_name in item_name:
                                    score += 80
                                elif any(
                                    word in item_name for word in search_words
                                ):
                                    common_words = search_words.intersection(
                                        item_words
                                    )
                                    score += len(common_words) * 20
                                open_vacancies = item.get('open_vacancies', 0)
                                score += min(open_vacancies, 10) * 2
                                if item.get('type') == 'company':
                                    score += 10
                                if score > best_score:
                                    best_score = score
                                    best_match = item
                            if best_match and best_score >= 40:
                                employer_url = best_match.get(
                                    'alternate_url', ''
                                )
                                employer_id = employer_url.split(
                                    '/'
                                )[-1] if employer_url else best_match['id']
                                result = (
                                    employer_id,
                                    best_match['name'],
                                    employer_url
                                )
                                self.employer_cache[cache_key] = result
                                return result
                async with self.session.get(
                    'https://api.hh.ru/employers',
                    params={
                        'text': company_name,
                        'area': '113',
                        'per_page': '10'
                    }
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        items = data.get('items', [])
                        if items:
                            employer_url = items[0].get('alternate_url', '')
                            employer_id = employer_url.split(
                                '/'
                            )[-1] if employer_url else items[0]['id']
                            result = (
                                employer_id, items[0]['name'], employer_url
                            )
                            self.employer_cache[cache_key] = result
                            return result
                return ('', '', '')
            except Exception as e:
                logger.error(f'Ошибка: {e}')
                return ('', '', '')

    async def search_support_vacancies(
        self,
        company_name,
        employer_id='',
        employer_url=''
    ):
        """Ищет вакансии поддержки для компании с улучшенным анализом"""
        cache_key = f'{company_name}_{employer_id}'
        if cache_key in self.vacancies_cache:
            return self.vacancies_cache[cache_key]
        vacancies_data = []
        all_vacancy_urls = []
        search_url = ''
        try:
            async with self.semaphore:
                if employer_id:
                    page = 0
                    per_page = 100
                    while True:
                        async with self.session.get(
                            'https://api.hh.ru/vacancies',
                            params={
                                'employer_id': employer_id,
                                'area': '113',
                                'per_page': str(per_page),
                                'page': str(page)
                            }
                        ) as response:
                            if response.status == 200:
                                data = await response.json()
                                items = data.get('items', [])
                                if not items:
                                    break
                                for item in items:
                                    title = item.get('name', '')
                                    snippet = item.get(
                                        'snippet', {}
                                    ).get('requirement', '') + ' ' + item.get(
                                        'snippet', {}
                                    ).get('responsibility', '')
                                    is_support, reason = self.support_vacancy(
                                        title, snippet
                                    )
                                    if is_support:
                                        vacancies_data.append({
                                            'title': title,
                                            'snippet': snippet,
                                            'url': item.get(
                                                'alternate_url',
                                                (
                                                    'https://hh.ru/vacancy/'
                                                    f'{item['id']}'
                                                )
                                            ),
                                            'id': item['id'],
                                            'reason': reason
                                        })
                                        all_vacancy_urls.append(item.get(
                                            'alternate_url',
                                            (
                                                'https://hh.ru/vacancy/'
                                                f'{item['id']}'
                                            )
                                        ))
                                pages = data.get('pages', 0)
                                page += 1
                                if page >= pages:
                                    break
                                await asyncio.sleep(0.1)
                        search_url = employer_url or (
                            f'https://hh.ru/employer/{employer_id}'
                        )
                if len(vacancies_data) < 5:
                    search_queries = (
                        f'{company_name} поддержка',
                        f'{company_name} оператор',
                        f'{company_name} контакт-центр',
                        f'{company_name} менеджер клиентов',
                    )
                    for query in search_queries[:2]:
                        async with self.session.get(
                            'https://api.hh.ru/vacancies',
                            params={
                                'text': query,
                                'area': '113',
                                'per_page': '30',
                                'page': '0',
                                'search_field': 'company_name'
                            }
                        ) as response:
                            if response.status == 200:
                                data = await response.json()
                                for item in data.get('items', []):
                                    title = item.get('name', '')
                                    snippet = item.get('snippet', {}).get(
                                        'requirement', ''
                                    ) + ' ' + item.get('snippet', {}).get(
                                        'responsibility', ''
                                    )
                                    employer_name = item.get(
                                        'employer', {}
                                    ).get('name', '').lower()
                                    if company_name.lower() in employer_name:
                                        is_support, reason = (
                                            self.support_vacancy(
                                                title, snippet
                                            )
                                        )
                                        if is_support:
                                            vac_id = item['id']
                                            if not any(
                                                v['id'] == vac_id
                                                for v in vacancies_data
                                            ):
                                                vacancies_data.append({
                                                    'title': title,
                                                    'snippet': snippet,
                                                    'url': item.get(
                                                        'alternate_url',
                                                        (
                                                            'https://hh.ru/vac'
                                                            f'ancy/{vac_id}'
                                                        )
                                                    ),
                                                    'id': vac_id,
                                                    'reason': reason
                                                })
                                                all_vacancy_urls.append(
                                                    item.get(
                                                        'alternate_url',
                                                        (
                                                            'https://hh.ru/vac'
                                                            f'ancy/{vac_id}'
                                                        )
                                                    )
                                                )
                                if search_url == '':
                                    search_url = (
                                        'https://hh.ru/search/vacancy?'
                                        f'text={query.replace(" ", "+")}'
                                    )
                                await asyncio.sleep(0.2)
        except Exception as e:
            logger.error(f'Ошибка: {e}')
        result = (vacancies_data, search_url)
        self.vacancies_cache[cache_key] = result
        return result

    async def analyze_company(self, name, inn, site):
        """Анализ компании."""
        try:
            employer_id, _, employer_url = await self.find_employer_id(name)
            vacancies_data, search_url = await self.search_support_vacancies(
                name, employer_id, employer_url
            )
            vacancies_analysis = self.analyze_vacancies_set(vacancies_data)
            evidence_type = ''
            team_size = 0
            evidence_text = ''
            evidence_url = ''
            if vacancies_analysis['team_size'] >= 10:
                team_size = vacancies_analysis['team_size']
                evidence_text = vacancies_analysis['evidence']
                evidence_url = (
                    search_url or employer_url
                    or f'https://hh.ru/search/vacancy?text={name}+поддержка'
                )
                evidence_type = 'vacancies_estimate'
                if 'прямое упоминание' in evidence_text:
                    evidence_type = 'vacancies_direct'
                else:
                    evidence_type = 'vacancies_estimate'
            elif vacancies_analysis['team_size'] >= 10:
                team_size = vacancies_analysis['team_size']
                evidence_text = vacancies_analysis['evidence']
                evidence_url = (
                    search_url or employer_url
                    or f'https://hh.ru/search/vacancy?text={name}+поддержка'
                )
                evidence_type = 'vacancies_estimate'
            if team_size < 10:
                return None
            result = ValidCompanyResult(
                name=name,
                inn=inn,
                site=site,
                support_team_size_min=team_size,
                support_evidence=evidence_text,
                evidence_url=evidence_url,
                evidence_type=evidence_type,
                source='jobs_analysis',
                hh_employer_id=employer_id,
                support_vacancies_count=len(vacancies_data),
                support_vacancy_titles=[
                    v['title'] for v in vacancies_data[:15]
                ],
                vacancies_sample_urls=[v['url'] for v in vacancies_data[:3]],
                mentions_24_7=vacancies_analysis['mentions_24_7'],
                shift_work_mentioned=vacancies_analysis['shift_work'],
                job_titles_found='; '.join(
                    [v['title'][:50] for v in vacancies_data[:5]]
                ) if vacancies_data else '',
                jobs_url=search_url,
                estimated_team_from_jobs=vacancies_analysis['team_size'],
                jobs_evidence=vacancies_analysis['evidence']
            )
            return result
        except Exception as e:
            error_result = ValidCompanyResult(
                name=name,
                inn=inn,
                site=site,
                support_team_size_min=0,
                support_evidence='',
                evidence_url='',
                evidence_type='error',
                source='jobs_analysis',
                error=str(e),
                parsed_successfully=False
            )
            return error_result


def load_companies_from_csv(filepath=RAW_DIR/FILENAME_FOR_CANDIDATES):
    """Загружает компании из CSV-файла."""
    try:
        df = pd.read_csv(filepath)
        if 'site' not in df.columns:
            raise ValueError('CSV должен содержать колонку "site"')
        companies = []
        for _, row in df.iterrows():
            if (not isinstance(site := row['site'], str)) or not site.strip():
                continue
            companies.append({
                'inn': str(row['inn']),
                'name': str(row['name']),
                'site': site.strip()
            })
        return companies
    except Exception as e:
        logger.error(f'Ошибка загрузки CSV {filepath}: {e}')
        return []


async def analyze_companies_batch(
        companies,
        analyzer=HHSupportAnalyzer,
        batch_size=10,
        delay=1.0
):
    """Анализирует партию компаний с улучшенным управлением."""
    valid_results = []
    error_results = []
    total = len(companies)
    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batch = companies[batch_start:batch_end]
        batch_tasks = []
        for company in batch:
            task = asyncio.create_task(
                analyzer.analyze_company(
                    name=company['name'],
                    inn=company['inn'],
                    site=company['site']
                )
            )
            batch_tasks.append(task)
            await asyncio.sleep(0.1)
        batch_results = await asyncio.gather(
            *batch_tasks, return_exceptions=True
        )
        for result in batch_results:
            if isinstance(result, Exception):
                continue
            if result is None:
                continue
            if (
                result.parsed_successfully
                and result.support_team_size_min >= 10
            ):
                valid_results.append(result)
            elif not result.parsed_successfully:
                error_results.append(result)
        if batch_end < total:
            await asyncio.sleep(delay)
    return valid_results


async def main():
    """Основная функция запуска."""
    try:
        companies = await load_companies_from_csv()
        async with HHSupportAnalyzer(
            max_concurrent=4,
            request_timeout=25
        ):
            valid_results = await analyze_companies_batch(companies)
        if valid_results:
            df = pd.DataFrame([r.to_dict() for r in valid_results])
            df = df[df['support_team_size_min'] >= 10]
            df.to_csv(
                RAW_DIR / FILENAME_FOR_PARSE_JOBS,
                index=False,
                encoding='utf-8-sig'
            )
    except Exception as e:
        logger.error(f"Ошибка создания отчета: {e}")
