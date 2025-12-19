import re

import pandas as pd

from . import (
    logger,
    FILENAME_FOR_MERGE_DATA,
    FILENAME_FOR_PARSE_JOBS,
    FILENAME_FOR_PARSE_SITES,
    RAW_DIR
)


class DataNormalizer:
    """Класс для нормализации и дедупликации данных."""

    def __init__(self):
        self.raw_dir = RAW_DIR

    def load_source_data(self):
        """Загружает исходные данные."""
        try:
            sites_path = self.raw_dir / FILENAME_FOR_PARSE_SITES
            jobs_path = self.raw_dir / FILENAME_FOR_PARSE_JOBS
            sites_df = pd.read_csv(sites_path, encoding='utf-8-sig')
            jobs_df = pd.read_csv(
                jobs_path, encoding='utf-8-sig'
            ) if jobs_path.exists() else pd.DataFrame()
            return sites_df, jobs_df
        except Exception as e:
            logger.error(f'Ошибка загрузки данных: {e}')
            return pd.DataFrame(), pd.DataFrame()

    def clean_inn(self, inn_value):
        """Очистка ИНН."""
        if pd.isna(inn_value) or inn_value == '':
            return ''
        inn_str = str(inn_value)
        inn_clean = re.sub(
            r'\D', '', inn_str[:-2] if inn_str.endswith('.0') else inn_str
        )
        return inn_clean if len(
            inn_clean
        ) in (10, 12) and inn_clean.isdigit() else inn_clean

    def normalize_company_name(self, name):
        """Нормализация названия компании для сопоставления."""
        if pd.isna(name) or not isinstance(name, str):
            return ''
        normalized = name.lower().strip()
        legal_forms = (
            'ооо', 'зао', 'оао', 'пао', 'ао', 'ип',
            'нко', 'мкк', 'ltd', 'llc', 'inc', 'gmbh'
        )
        for form in legal_forms:
            normalized = normalized.replace(form, '')
        normalized = re.sub(r'[«»"\'()\[\]{}<>]', '', normalized)
        return re.sub(r'\s+', ' ', normalized).strip()

    def merge_and_dedup(self, sites_df=pd.DataFrame, jobs_df=pd.DataFrame):
        """Объединение и дедупликация данных из двух источников."""
        base_companies = {}
        for _, row in jobs_df.iterrows():
            if inn := self.clean_inn(row.get('inn', '')):
                base_companies[inn] = {
                    'inn': inn,
                    'name': row.get('name', ''),
                    'site': row.get('site', ''),
                    'support_team_size_min': row.get(
                        'support_team_size_min', 0
                    ),
                    'support_evidence': row.get('support_evidence', ''),
                    'evidence_url': row.get('evidence_url', ''),
                    'evidence_type': row.get(
                        'evidence_type', 'vacancies_estimate'
                    ),
                    'source': row.get('source', 'jobs_analysis'),
                    **{k: row[k] for k in row.index if k not in (
                        'site', 'support_evidence', 'evidence_url', 'source',
                        'inn', 'support_team_size_min', 'name', 'evidence_type'
                    ) and pd.notna(row[k])}
                }
        for _, row in sites_df.iterrows():
            inn = self.clean_inn(row.get('inn', ''))
            if not inn:
                continue
            site_size = row.get('support_team_size_min', 0)
            if inn in base_companies:
                existing_size = base_companies[inn].get(
                    'support_team_size_min', 0
                )
                if site_size > existing_size:
                    base_companies[inn].update({
                        'support_team_size_min': site_size,
                        'support_evidence': row.get('support_evidence', ''),
                        'evidence_url': row.get('evidence_url', ''),
                        'evidence_type': row.get(
                            'evidence_type', 'site_estimate'
                        ),
                        'source': 'combined'
                    })
            else:
                base_companies[inn] = {
                    'inn': inn,
                    'name': row.get('name', ''),
                    'site': row.get('site', ''),
                    'support_team_size_min': site_size,
                    'support_evidence': row.get('support_evidence', ''),
                    'evidence_url': row.get('evidence_url', ''),
                    'evidence_type': row.get('evidence_type', 'site_estimate'),
                    'source': row.get('source', 'sites_analysis'),
                    **{k: row[k] for k in row.index if k not in [
                        'site', 'support_evidence', 'evidence_url', 'source',
                        'inn', 'support_team_size_min', 'name', 'evidence_type'
                    ] and pd.notna(row[k])}
                }
        result_df = pd.DataFrame(list(base_companies.values()))
        if (
            not result_df.empty and 'support_team_size_min'
            in result_df.columns
        ):
            result_df = result_df[result_df['support_team_size_min'] >= 10]
        return result_df

    def clean_final_dataset(self, df=pd.DataFrame):
        """Очистка финального датасета."""
        if df.empty:
            return df
        cleaned_df = df.copy()
        if 'inn' in cleaned_df.columns:
            cleaned_df['inn'] = cleaned_df['inn'].apply(self.clean_inn)
        if 'evidence_url' in cleaned_df.columns:
            cleaned_df['evidence_url'] = cleaned_df.apply(
                lambda row: self._fix_evidence_url(row), axis=1
            )
        bool_fields = (
            'has_support_email', 'has_contact_form', 'has_online_chat',
            'has_messengers', 'has_support_section',
            'has_kb_or_faq', 'mentions_24_7'
        )
        for field in bool_fields:
            if field in cleaned_df.columns:
                cleaned_df[field] = cleaned_df[field].apply(
                    lambda x: 'true' if str(x).lower() in (
                        'true', '1', 'yes', 'да', 't'
                    ) else 'false'
                )
        required_fields = (
            'inn', 'name', 'site', 'support_team_size_min', 'support_evidence',
            'evidence_url', 'evidence_type', 'source', 'has_support_email',
            'has_contact_form', 'has_online_chat', 'has_messengers',
            'has_support_section', 'has_kb_or_faq', 'mentions_24_7'
        )
        optional_fields = (
            'revenue', 'employees', 'okved_main', 'support_email',
            'support_url', 'kb_url', 'chat_vendor'
        )
        final_fields = [
            f for f in required_fields if f in cleaned_df.columns
        ]
        final_fields.extend(
            [f for f in optional_fields if f in cleaned_df.columns]
        )
        final_fields.extend(
            [f for f in cleaned_df.columns if f not in final_fields]
        )
        cleaned_df = cleaned_df[final_fields].sort_values(
            ['support_team_size_min', 'name'], ascending=[False, True]
        ).reset_index(drop=True)
        return cleaned_df

    def _fix_evidence_url(self, row):
        """Исправление evidence_url."""
        evidence_url = str(row.get('evidence_url', ''))
        if not evidence_url or evidence_url.lower() in ['nan', 'none']:
            if str(row.get(
                'evidence_type', ''
            )) == 'site' and 'Уровень B' in str(row.get(
                'support_evidence', ''
            )):
                if pd.notna(row.get('support_url')) and str(row.get(
                    'support_url', ''
                )) != '':
                    return row['support_url']
                elif pd.notna(row.get('kb_url')) and str(
                    row.get('kb_url', '')
                ) != '':
                    return row['kb_url']
                else:
                    return row.get('site', '')
        return evidence_url

    def save_result(self, df=pd.DataFrame, filename=FILENAME_FOR_MERGE_DATA):
        """Сохранение результата."""
        try:
            output_path = self.raw_dir / filename
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            return True
        except Exception as e:
            logger.error(f'Ошибка сохранения: {e}')
            return False


def main():
    """Основная функция."""
    try:
        normalizer = DataNormalizer()
        sites_df, jobs_df = normalizer.load_source_data()
        if not jobs_df.empty:
            jobs_df['normalized_name'] = jobs_df[
                'name'
            ].apply(normalizer.normalize_company_name)
        merged_df = normalizer.merge_and_dedup(sites_df, jobs_df)
        if merged_df.empty:
            logger.error('Нет компаний, соответствующих критерию 10+ человек')
            return None
        final_df = normalizer.clean_final_dataset(merged_df)
        normalizer.save_result(final_df)
        return final_df
    except Exception as e:
        logger.error(f'Ошибка: {e}')
        return None
