import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

RAW_DIR = PROJECT_ROOT / 'data' / 'raw'
FILENAME_FOR_CANDIDATES = 'candidates.csv'
FILENAME_FOR_PARSE_SITES = 'sites_analysis.csv'
FILENAME_FOR_PARSE_JOBS = 'jobs_analysis.csv'
FILENAME_FOR_MERGE_DATA = 'merged_companies.csv'
FINAL_FILENAME = 'companies.csv'
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        ' (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
}

log_dir = PROJECT_ROOT / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(
        log_dir / 'parser.log',
        encoding='utf-8'
    )]
)

logger = logging.getLogger(__name__)
