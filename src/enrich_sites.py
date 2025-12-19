import asyncio
import re
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup
import pandas as pd

from . import (
    logger,
    FILENAME_FOR_CANDIDATES,
    FILENAME_FOR_PARSE_SITES,
    HEADERS,
    RAW_DIR
)

SUPPORT_KEYWORDS = (
    'поддерж', 'помощь', 'контакт', 'служб', 'сервис', 'техподдерж',
    'справк', 'консульт', 'обслуживани', 'обратн', 'связ',
    'обращен', 'заявк', 'сообщен', 'письм', 'обратн',
    'отдел поддерж', 'служба поддерж', 'центр поддерж',
    'линия поддерж', 'канал поддерж', 'поддержка клиент',
    'клиентск поддерж', 'клиентск сервис',
    'контакт-центр', 'колл-центр', 'call-центр', 'контакт центр',
    'диспетчер', 'оператор', 'приём звонк', 'звонк',
    'напиши', 'задай вопрос', 'написать нам', 'свяжитесь',
    'позвони', 'позвоните', 'звоните', 'пишите',
    'страница поддерж', 'раздел поддерж', 'центр помощи',
    'помощь клиент', 'поддержка пользовател',
    'support', 'help', 'contact', 'service', 'customer',
    'assist', 'aid', 'care', 'backup', 'guidance',
    'customer care', 'customer service', 'client support',
    'technical support', 'tech support', 'it support',
    'contact us', 'get in touch', 'reach us', 'get help',
    'call us', 'phone', 'email us', 'write to us',
    'chat with us', 'live chat', 'online chat',
    'help center', 'support center', 'service center',
    'contact center', 'call center', 'customer center',
    'support page', 'help page', 'contact page',
    'customer portal', 'client portal', 'help desk'
)
FAQ_KEYWORDS = (
    'faq', 'часто задаваем', 'база знаний', 'вопрос', 'ответ',
    'часто задаваемые вопросы', 'чаво', 'частые вопросы',
    'вопросы и ответы', 'вопросы-ответы', 'популярные вопросы',
    'часто спрашивают', 'спрашивают', 'отвечаем',
    'база знаний', 'знания', 'knowledge base', 'knowledge',
    'статьи', 'инструкц', 'руководств', 'гайд', 'руководство',
    'помощь', 'справочн', 'документац', 'мануал', 'manual',
    'how to', 'how-to', 'как пользоваться', 'как настроить',
    'раздел вопрос', 'центр знаний', 'помощь и поддержка',
    'справочный центр', 'информационный раздел',
    'искать в базе', 'поиск по базе', 'найти ответ',
    'решение проблем', 'troubleshooting', 'решение'
)
MESSENGERS = (
    'telegram', 'whatsapp', 'viber', 'vk.com', 'vkontakte', 'skype', 'max'
)
CHAT_INDICATORS = (
    'chat', 'чат', 'онлайн-чат', 'livechat', 'jivo', 'livetex',
    'tawk.to', 'zammad', 'crisp.chat', 'intercom', 'drift',
    'freshchat', 'zendesk.chat', 'tidio.chat', 'chatra',
    'widget.createsend', 'chat-button', 'chat-widget', 'chatbot',
    't.me/widget', 'whatsapp-chat', 'viber-chat'
)
JOB_KEYWORDS = (
    'ваканс', 'работа', 'карьер', 'трудойстройств',
    'vacancy', 'job', 'career', 'employment', 'work',
    'работа у нас', 'работа в компании', 'присоединиться к команде',
    'команда', 'коллектив', 'сотрудничество', 'стажировк',
    'работать с нами', 'мы ищем', 'ищем сотрудник',
    'открытые ваканс', 'актуальные ваканс', 'поиск сотрудник',
    'трудоустройств', 'рабочие мест', 'персонал',
    'приглашаем на работу', 'присоединяйся', 'команда профессионал',
    'join us', 'join our team', 'we are hiring', 'hiring',
    'open positions', 'job openings', 'current openings',
    'opportunities', 'recruitment', 'recruiting', 'staff',
    'team', 'work with us', 'apply now', 'apply today',
    'работа в', 'карьера в', 'вакансии компании',
    'страница ваканс', 'раздел ваканс', 'раздел карьер',
    'careers at', 'jobs at', 'work at',
    'кандидат', 'резюме', 'требован', 'обязанност',
    'условия работы', 'зарплат', 'оклад', 'график',
    'candidate', 'resume', 'requirements', 'responsibilities',
    'salary', 'benefits', 'schedule',
    '/vacancy', '/job', '/career', '/rabota', '/jobs',
    '/vacancies', '/careers', '/employment', '/work',
    'вакансии/', 'работа/', 'карьера/'
)
SUPPORT_JOB_PATTERNS = (
    r'поддержк[а-яё]*', r'helpdesk', r'service\s*desk',
    r'тех[.-]*поддержк[а-яё]*', r'саппорт', r'инженер.*поддержк',
    r'оператор', r'консультант', r'менеджер.*клиент',
    r'специалист\s+поддержки', r'агент\s+поддержки',
    r'support\s+agent', r'техническ[а-яё]*\s+поддержк[а-яё]*',
    r'сотрудник.*поддержк', r'представитель.*поддержк',
    r'служб[а-яё]*\s+поддержк[а-яё]*', r'отдел[а-яё]*\s+поддержк[а-яё]*',
    r'контакт[-\s]*центр', r'call\s*center', r'call.*центр',
    r'клиентск[а-яё]*\s+поддержк[а-яё]*', r'customer\s*support',
    r'обслуживани[ея]\s+клиент',
    r'L[123]', r'1[-\s]*я\s+линия', r'2[-\s]*я\s+линия',
    r'первая\s+линия', r'вторая\s+линия',
    r'technical\s+support', r'it\s+support', r'client\s+support',
    r'user\s+support', r'customer\s+care', r'customer\s+service'
)


def extract_support_email(text):
    """Извлекает email."""
    if priority_match := re.search(
        r'\b(support|help)@[\w\.-]+\.[a-z]{2,}\b', text, re.IGNORECASE
    ):
        return priority_match.group(0), True
    if other_match := re.search(
        r'\b(info|feedback|service|contact)@[\w\.-]+\.[a-z]{2,}\b',
        text, re.IGNORECASE
    ):
        return other_match.group(0), False
    return '', False


def find_size_team_mention(text, page_url):
    """Ищет упоминания размера команды поддержки."""
    patterns = (
        (
            r'(?:в\s+)?(?:служб[еа]|команд[еа]|отдел[еа]|штат[е]?\s+)?поддерж'
            r'(?:ки|ке|ка|ку)[\s\w,.-]{0,30}?(\d{2,})\s*'
            r'(?:человек|сотрудник|специалист|оператор)'
        ),
        (
            r'контакт[-\s]*центр[а-я]*[\s\w,.-]{0,30}?(\d{2,})\s*'
            r'(?:человек|сотрудник|оператор|работник)'
        ),
        (
            r'(?:насчитывает|составляет|всего|более|около)\s+(\d{2,})\s*'
            r'(?:человек|сотрудник)[\s\w,.-]{0,20}(?:в\s+)?поддерж'
        ),
        (
            r'поддерж(?:ка|ки|ке)[\s\w,.-]{0,30}?из\s+'
            r'(\d{2,})\s*(?:человек|сотрудник)'
        ),
        (
            r'(\d{2,})\s*(?:человек|сотрудник)'
            r'[\s\w,.-]{0,15}работает?\s+в\s+поддерж'
        ),
    )
    if not any(word in text.lower() for word in SUPPORT_KEYWORDS):
        return 0, '', ''
    for pattern in patterns:
        match = re.search(pattern, text.lower())
        if match:
            try:
                size = int(match.group(1))
                if 10 <= size <= 500:
                    context = text[max(0, match.start() - 100):min(
                            len(text), match.end() + 100
                    )]
                    if any(
                        word in context.lower() for word in SUPPORT_KEYWORDS
                    ):
                        clean_context = ' '.join(context.replace(
                            '\n', ' '
                        ).split()[:30]) + '...'
                        return (
                            size, page_url,
                            f'Уровень A: прямое упоминание "{clean_context}"'
                        )
            except (ValueError, IndexError, AttributeError):
                continue
    return 0, '', ''


def estimate_size_team_level_b(page_data, result):
    """Анализ уровня В."""
    evidence = []
    team_size = 0
    if page_data.get('mentions_24_7') and page_data.get(
        'shift_work_mentioned'
    ):
        evidence.append('поддержка 24/7 со сменным графиком')
        team_size = 10
    job_titles = page_data.get('job_titles_found', [])
    if page_data.get('company_site_vacancies', 0) >= 2:
        unique_titles_count = len({title for title in job_titles if title})
        if unique_titles_count >= 2:
            evidence.append(f'{unique_titles_count} разных вакансий поддержки')
            if page_data.get('jobs_url'):
                result['evidence_url'] = page_data['jobs_url']
            team_size = max(team_size if 'team_size' in locals() else 0, 10)
    if job_titles:
        titles_lower = [str(title).lower() for title in job_titles]
        has_l1_l2 = any(
            'l1' in t or 'l2' in t or 'уровень' in t for t in titles_lower
        )
        has_phone = any(
            'телефон' in t or 'звонк' in t or 'call' in t for t in titles_lower
        )
        has_chat = any(
            'чат' in t or 'chat' in t or 'online' in t for t in titles_lower
        )
        has_qa = any(
            'qa' in t or 'контрол' in t or 'оценк' in t for t in titles_lower
        )
        found_roles = []
        if has_l1_l2:
            found_roles.append('многоуровневая поддержка')
        if has_phone:
            found_roles.append('телефонная поддержка')
        if has_chat:
            found_roles.append('чат-поддержка')
        if has_qa:
            found_roles.append('контроль качества')
        if len(found_roles) >= 2:
            evidence.append(f'разные роли ({", ".join(found_roles)})')
            team_size = max(team_size, 10)
    indicators = (
        'тысяч обращений', 'сотен обращений', 'высокая нагрузка',
        'много клиентов', 'большой поток', 'крупный контакт-центр',
        'обслуживаем тысячи', 'обрабатываем сотни'
    )
    if found_load := [
        indicator for indicator in indicators if indicator in page_data.get(
            'full_text', ''
        ).lower()
    ]:
        evidence.append(f'признаки высокой нагрузки ({found_load[0]})')
        team_size = max(team_size if 'team_size' in locals() else 0, 10)
    if 'team_size' not in locals() or team_size < 10 or not evidence:
        return 0, '', ''
    if evidence and page_data.get('has_kb_or_faq'):
        evidence.append('база знаний/FAQ')
    return team_size, result['site'], (
        f'Уровень B: оценка основана на: {", ".join(evidence)}'
    )


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


class AsyncSiteEnricher:
    """Асинхронный парсинг сайтов компаний."""

    async def enrich_companies(self, companies_data):
        async def process_with_semaphore(company_data):
            async with asyncio.Semaphore(5):
                await asyncio.sleep(1.5)
                return await self.enrich_company(**company_data)
        tasks = [process_with_semaphore(company) for company in companies_data]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results

    async def enrich_company(self, inn, name, site):
        """Анализирует сайт компании на признаки службы поддержки."""
        result = {
            'inn': str(inn).strip(),
            'name': str(name).strip(),
            'site': site,
            'support_team_size_min': 0,
            'support_evidence': '',
            'evidence_url': '',
            'evidence_type': 'other',
            'source': 'company_site',
            'has_support_email': False,
            'has_contact_form': False,
            'has_online_chat': False,
            'has_messengers': False,
            'has_support_section': False,
            'has_kb_or_faq': False,
            'mentions_24_7': False,
            'support_email': '',
            'support_url': '',
            'kb_url': '',
            'chat_vendor': '',
            'company_site_vacancies': 0,
            'job_titles_found': [],
            'jobs_url': '',
            'shift_work_mentioned': False,
            'error': '',
            'parsed_successfully': False
        }
        try:
            page_data = await self.fetch_and_parse_page(result['site'])
            for key in (
                'has_support_email', 'has_contact_form', 'has_online_chat',
                'has_messengers', 'has_support_section', 'has_kb_or_faq',
                'mentions_24_7', 'support_email', 'support_url', 'kb_url',
                'chat_vendor', 'company_site_vacancies', 'job_titles_found',
                'jobs_url', 'shift_work_mentioned'
            ):
                if key in page_data:
                    result[key] = page_data[key]
            result['parsed_successfully'] = True
            all_texts = []
            if page_data.get('full_text'):
                all_texts.append((page_data['full_text'], result['site']))
            if page_data.get(
                'support_section_text'
            ) and page_data.get('support_url'):
                all_texts.append((
                    page_data['support_section_text'],
                    page_data['support_url']
                ))
            for text_source, source_url in all_texts:
                team_size, evidence_url, evidence = find_size_team_mention(
                    text_source, source_url
                )
                if team_size:
                    result['support_team_size_min'] = team_size
                    result['support_evidence'] = evidence
                    result['evidence_url'] = evidence_url
                    result['evidence_type'] = 'site'
                    break
            if result['support_team_size_min'] < 10:
                estimated_size, evidence_url_b, evidence_b = (
                    estimate_size_team_level_b(page_data, result)
                )
                if estimated_size:
                    result['support_team_size_min'] = estimated_size
                    result['support_evidence'] = evidence_b
                    result['evidence_url'] = evidence_url_b
                    result['evidence_type'] = 'site'
        except Exception as e:
            result['error'] = f'{type(e).__name__}: {str(e)}'
            result['parsed_successfully'] = False
            logger.error(f'Ошибка при обработке {name}: {result['error']}')
        return result

    async def fetch_page_text(self, url):
        """Загружает текст страницы асинхронно."""
        try:
            async with aiohttp.ClientSession(headers={
                **HEADERS,
                'Accept': (
                    'text/html,application/xhtml+xml'
                    ',application/xml;q=0.9,*/*;q=0.8'
                )
            }) as session:
                async with session.get(url, ssl=False) as response:
                    if response.status == 200:
                        html = await response.text()
                        if html and len(html) > 100:
                            return html
                        else:
                            return ''
                    else:
                        return ''
        except Exception:
            return ''

    async def _find_career_page(self, base_url):
        """Ищет страницу вакансий на сайте компании."""
        try:
            career_patterns = (
                '/career', '/jobs', '/vacancies', '/vacancy', '/rabota',
                '/about/career', '/company/career', '/company/jobs',
                '/hr', '/work', '/team', '/careers',
                '/вакансии', '/карьера', '/работа'
            )
            for pattern in career_patterns:
                url = urljoin(base_url.rstrip('/') + '/', pattern.lstrip('/'))
                async with aiohttp.ClientSession(headers={
                    **HEADERS,
                    'Accept': (
                        'text/html,application/xhtml+xml,'
                        'application/xml;q=0.9,*/*;q=0.8'
                    )
                }) as session:
                    async with session.head(
                        url, ssl=False, allow_redirects=True
                    ) as response:
                        if response.status == 200:
                            return url
            if html := await self.fetch_page_text(base_url):
                soup = BeautifulSoup(html, 'html.parser')
                for link in soup.find_all('a', href=True, limit=50):
                    if any(keyword in link.get_text(
                        strip=True
                    ).lower() for keyword in JOB_KEYWORDS):
                        return urljoin(base_url, link['href'])
        except Exception:
            pass
        return None

    async def _parse_vacancies_from_page(self, url):
        """Парсит вакансий на сайте компании."""
        result = {
            'vacancies_found': 0,
            'titles': [],
            'shift_work': False
        }
        try:
            html = await self.fetch_page_text(url)
            if not html:
                return result
            shift_regex = re.compile(
                r'24[/×]7|круглосуточно|сменн[а-яё]*\s+график|'
                r'2/2|3/3|ночн[а-яё]*\s+смен[ау]?|посменн[а-яё]*|'
                r'сменн[а-яё]+\s+работ',
                re.IGNORECASE
            )
            if shift_regex.search(html.lower()):
                result['shift_work'] = True
            soup = BeautifulSoup(html, 'html.parser')
            vacancy_candidates = []
            for tag in soup.find_all(('h1', 'h2', 'h3', 'h4')):
                text = tag.get_text(strip=True)
                if text and 10 < len(text) < 100:
                    vacancy_candidates.append(text)
            for link in soup.find_all('a', href=True):
                text = link.get_text(strip=True)
                href = link['href'].lower()
                if (text and 5 < len(text) < 80 and (
                    'ваканс' in href or 'vacancy' in href or 'job' in href or
                    'работа' in href or 'career' in href
                )):
                    vacancy_candidates.append(text)
            for elem in soup.find_all(('div', 'li', 'span')):
                if any(vac_word in ' '.join(elem.get('class', [])).lower(
                ) for vac_word in (
                    'vacancy', 'job', 'position', 'vacans', 'rabota'
                )):
                    text = elem.get_text(strip=True)
                    if text and 10 < len(text) < 150:
                        vacancy_candidates.append(text)
            real_vacancies = self._clean_and_filter_vacancies(
                vacancy_candidates
            )
            result['vacancies_found'] = len(real_vacancies)
            result['titles'] = real_vacancies[:15]
        except Exception:
            pass
        return result

    async def fetch_and_parse_page(self, url):
        """Асинхронно загружает страницу и извлекает признаки."""
        data = {
            'full_text': '',
            'support_section_text': '',
            'has_support_email': False,
            'has_contact_form': False,
            'has_online_chat': False,
            'has_messengers': False,
            'has_support_section': False,
            'has_kb_or_faq': False,
            'mentions_24_7': False,
            'support_email': '',
            'support_url': '',
            'kb_url': '',
            'chat_vendor': '',
            'company_site_vacancies': 0,
            'job_titles_found': [],
            'jobs_url': '',
            'shift_work_mentioned': False
        }
        try:
            html = await self.fetch_page_text(url)
            if not html:
                return data
            soup = BeautifulSoup(html, 'html.parser')
            data['full_text'] = html
            visible_text = soup.get_text(separator=' ', strip=True)
            data['page_text'] = visible_text[:5000]
            email_pattern = (
                r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
            )
            text_emails = re.findall(email_pattern, html)
            mailto_emails = [
                a['href'][7:]
                for a in soup.find_all('a', href=True)
                if a['href'].startswith('mailto:')
            ]
            all_emails = text_emails + mailto_emails
            if all_emails:
                support_emails = [e for e in all_emails if e.lower(
                ).startswith(('support@', 'help@'))]
                if support_emails:
                    data['has_support_email'] = True
                    data['support_email'] = support_emails[0]
                else:
                    data['has_support_email'] = True
                    data['support_email'] = all_emails[0]
            if soup.find_all('form'):
                form_text = str(soup).lower()
                if any(keyword in form_text for keyword in (
                    'form', 'contact', 'обратн', 'заявк')
                ):
                    data['has_contact_form'] = True
            for vendor in CHAT_INDICATORS:
                if vendor in html.lower():
                    data['has_online_chat'] = True
                    data['chat_vendor'] = vendor
                    break
            for messenger in MESSENGERS:
                if messenger in html.lower():
                    data['has_messengers'] = True
                    break
            for link in soup.find_all('a', href=True, limit=100):
                if (any(k in link.get_text(strip=True).lower(
                ) for k in SUPPORT_KEYWORDS) or any(k in link['href'].lower(
                ) for k in ['support', 'help', 'contact', 'faq'])):
                    data['has_support_section'] = True
                    data['support_url'] = urljoin(url, link['href'])
                    break
            for link in soup.find_all('a', href=True, limit=50):
                if any(keyword in link.get_text(strip=True).lower(
                ) for keyword in FAQ_KEYWORDS):
                    data['has_kb_or_faq'] = True
                    data['kb_url'] = urljoin(url, link['href'])
                    break
            if '24/7' in html.lower() or 'круглосуточно' in html.lower():
                data['mentions_24_7'] = True
            career_page = await self._find_career_page(url)
            if career_page:
                data['jobs_url'] = career_page
                vacancies_data = await self._parse_vacancies_from_page(
                    career_page
                )
                data['company_site_vacancies'] = vacancies_data[
                    'vacancies_found'
                ]
                data['job_titles_found'] = vacancies_data['titles']
                data['shift_work_mentioned'] = vacancies_data['shift_work']
        except Exception:
            pass
        return data

    def _clean_and_filter_vacancies(self, job_titles):
        """Очищает и фильтрует список вакансий, оставляя только реальные."""
        real_vacancies = set()
        for title in job_titles:
            if not title or len(title) < 5:
                continue
            exclude_patterns = (
                r'©', r'copyright', r'все права', r'политика',
                r'конфиденциальност', r'карта сайта', r'cookie',
                r'использование файлов', r'пример', r'образец',
                r'продукт', r'услуг', r'решен', r'тариф', r'цена',
                r'контакт', r'о компани', r'отзыв', r'новост', r'блог',
                r'документ', r'инструкц', r'faq', r'база знаний',
                r'компенсац', r'льгот', r'преимуществ', r'бонус',
                r'забота', r'поддержк.*сем', r'материальн.*поддержк',
                r'шаблон', r'тестов', r'демо', r'социальн.*поддержк'
            )
            if any(re.search(
                pattern, title.lower()
            ) for pattern in exclude_patterns):
                continue
            if not any(re.search(
                pattern, title.lower()
            ) for pattern in SUPPORT_JOB_PATTERNS):
                continue
            real_vacancies.add(' '.join(title.split()[:8]))
        return list(real_vacancies)


async def run_async_enrichment(output_file=RAW_DIR/FILENAME_FOR_PARSE_SITES):
    """Основная асинхронная функция запуска парсинга."""
    companies_data = load_companies_from_csv()
    if not companies_data:
        logger.info('Нет компаний для обработки.')
        return None
    results = await AsyncSiteEnricher().enrich_companies(companies_data)
    final_df = pd.DataFrame(results)
    if not final_df.empty:
        bool_cols = (
            'has_support_email', 'has_contact_form', 'has_online_chat',
            'has_messengers', 'has_support_section', 'has_kb_or_faq',
            'mentions_24_7', 'shift_work_mentioned', 'parsed_successfully'
        )
        for col in bool_cols:
            if col in final_df.columns:
                final_df[col] = final_df[col].astype(str)
        if 'job_titles_found' in final_df.columns:
            final_df['job_titles_found'] = final_df['job_titles_found'].apply(
                lambda x: '; '.join(x) if isinstance(
                    x, list
                ) else str(x) if x else ''
            )
        valid_companies = final_df[(
            (final_df['support_team_size_min'] >= 10) &
            (final_df['parsed_successfully'] == 'True')
        )].copy()
        if len(valid_companies) > 0:
            valid_companies.to_csv(
                output_file, index=False, encoding='utf-8-sig'
            )
        else:
            logger.info('Нет компаний с поддержкой 10+ для сохранения.')
    return final_df


async def main():
    """Основная функция запуска."""
    results_df = await run_async_enrichment()
    return results_df
