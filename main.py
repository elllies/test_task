import asyncio
from pathlib import Path
import sys

from src.enrich_jobs import main as run_jobs
from src.enrich_sites import main as run_sites
from src.export_csv import main as run_export
from src.merge_normalize import main as run_merge

sys.path.insert(0, str(Path(__file__).parent))


def run_all():
    """Запускает пайплайн."""
    async def run_parallel():
        task1 = asyncio.create_task(run_jobs())
        task2 = asyncio.create_task(run_sites())
        await asyncio.gather(task1, task2)
    try:
        asyncio.run(run_parallel())
        run_merge()
        run_export()
    except Exception:
        pass


if __name__ == "__main__":
    run_all()
