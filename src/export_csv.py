import re

import pandas as pd

from . import (
    logger,
    FILENAME_FOR_MERGE_DATA,
    FINAL_FILENAME,
    PROJECT_ROOT,
    RAW_DIR
)


class DataExporter:
    """Класс для экспорта финального датасета."""

    def __init__(self):
        self.data_dir = PROJECT_ROOT / 'data'
        self.raw_dir = RAW_DIR
        self.output_dir = self.data_dir
        self.MIN_COMPANIES = 50
        self.MIN_TEAM_SIZE = 10
        self.REQUIRED_FIELDS = (
            'inn', 'name', 'site', 'support_team_size_min',
            'support_evidence', 'evidence_url', 'evidence_type',
            'source', 'has_support_email', 'has_contact_form',
            'has_online_chat', 'has_messengers', 'has_support_section',
            'has_kb_or_faq', 'mentions_24_7'
        )
        self.OPTIONAL_FIELDS = (
            'revenue', 'employees', 'okved_main', 'support_email',
            'support_url', 'kb_url', 'chat_vendor'
        )

    def clean_inn(self, inn_value):
        """Очистка ИНН."""
        if pd.isna(inn_value) or inn_value == '':
            return ''
        inn_str = str(inn_value)
        if inn_str.endswith('.0'):
            inn_str = inn_str[:-2]
        inn_clean = re.sub(r'\D', '', inn_str)
        if len(inn_clean) in [10, 12] and inn_clean.isdigit():
            return inn_clean
        return inn_clean

    def clean_boolean(self, value):
        """Очистка булевых значений."""
        if pd.isna(value) or value == '':
            return 'false'
        value_str = str(value).strip().lower()
        if value_str in ('true', '1', 'yes', 'да', 't'):
            return 'true'
        if value_str in ('false', '0', 'no', 'нет', 'f'):
            return 'false'
        if isinstance(value, bool):
            return 'true' if value else 'false'
        return 'false'

    def fix_email(self, email):
        """Исправление email."""
        if pd.isna(email) or email == '':
            return ''
        email_str = str(email)
        return '' if '@2x.jpg' in email_str else email_str

    def clean_dataset(self, df=pd.DataFrame):
        """Основная очистка датасета."""
        if df.empty:
            return df
        cleaned_df = df.copy()
        if 'inn' in cleaned_df.columns:
            cleaned_df['inn'] = cleaned_df['inn'].apply(self.clean_inn)
        bool_fields = (
            'has_support_email', 'has_contact_form', 'has_online_chat',
            'has_messengers', 'has_support_section', 'has_kb_or_faq',
            'mentions_24_7'
        )
        for field in bool_fields:
            if field in cleaned_df.columns:
                cleaned_df[field] = cleaned_df[field].apply(self.clean_boolean)
        if 'support_email' in cleaned_df.columns:
            cleaned_df['support_email'] = cleaned_df[
                'support_email'
            ].apply(self.fix_email)
        if 'inn' in cleaned_df.columns:
            valid_inn_mask = cleaned_df['inn'].apply(
                lambda x: len(str(x)) in [10, 12] and str(x).isdigit()
            )
            cleaned_df = cleaned_df[valid_inn_mask]
        if 'support_team_size_min' in cleaned_df.columns:
            try:
                cleaned_df['support_team_size_min'] = pd.to_numeric(
                    cleaned_df['support_team_size_min'], errors='coerce'
                )
                size_mask = cleaned_df[
                    'support_team_size_min'
                ] >= self.MIN_TEAM_SIZE
                cleaned_df = cleaned_df[size_mask]
            except Exception:
                logger.warning(
                    'Не удалось преобразовать support_team_size_min'
                )
        if (
            'support_team_size_min' in cleaned_df.columns
            and not cleaned_df.empty
        ):
            cleaned_df = cleaned_df.sort_values(
                'support_team_size_min', ascending=False
            )
        return cleaned_df

    def generate_final_columns(self, df=pd.DataFrame):
        """Генерация финальных колонок в правильном порядке."""
        if df.empty:
            return df
        result_df = df.copy()
        all_fields = list(result_df.columns)
        final_fields = []
        for field in self.REQUIRED_FIELDS:
            if field in all_fields:
                final_fields.append(field)
                all_fields.remove(field)
        for field in self.OPTIONAL_FIELDS:
            if field in all_fields:
                final_fields.append(field)
                all_fields.remove(field)
        final_fields.extend(all_fields)
        return result_df[final_fields]

    def export_final_dataset(
        self,
        input_file=FILENAME_FOR_MERGE_DATA,
        output_file=FINAL_FILENAME
    ):
        """Экспорт финального датасета."""
        try:
            input_path = self.raw_dir / input_file
            if not input_path.exists():
                logger.error(f'Файл не найден: {input_path}')
                return False
            df = pd.read_csv(input_path, encoding='utf-8-sig')
            cleaned_df = self.clean_dataset(df)
            final_df = self.generate_final_columns(cleaned_df)
            output_path = self.output_dir / output_file
            final_df = final_df.astype(str)
            if 'inn' in final_df.columns:
                final_df['inn'] = final_df['inn'].str.replace(
                    r'\.0$', '', regex=True
                )
            final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            return True
        except Exception as e:
            logger.error(f'Ошибка при экспорте: {e}')
            return False


def main():
    """Основная функция."""
    exporter = DataExporter()
    exporter.export_final_dataset()
