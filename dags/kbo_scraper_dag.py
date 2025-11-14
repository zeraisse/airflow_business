import textwrap
from datetime import datetime, timedelta

# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG

# --- VARIABLES SPÉCIFIQUES À VOTRE SCRAPPER ---
# 1. Chemin où le scrapper est monté dans le conteneur Airflow (Partie droite de votre volume)
SCRAPPER_DIR = '/opt/airflow/scrapers/kbo_scraper' 
# 2. Le nom de votre spider Scrapy (à remplacer)
SPIDER_NAME = 'kbo_company' 
# ---------------------------------------------

with DAG(
    "kbo_scraper_daily_run", 
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="DAG pour exécuter le scrapper KBO quotidiennement.",
    schedule=timedelta(days=1),  # Lancement quotidien
    start_date=datetime(2024, 1, 1),  # Démarre à partir du 1er janvier 2024
    catchup=False,
    tags=["scraper", "kbo"],
) as dag:

    # Tâche unique pour exécuter le scrapper Scrapy
    run_scrapy_job = BashOperator(
        task_id="launch_kbo_scraper",
        # La commande qui exécute le scrapper :
        # 1. Nous naviguons dans le répertoire du projet Scrapy.
        # 2. Nous lançons la commande 'scrapy crawl' pour exécuter le spider.
        bash_command=f"cd {SCRAPPER_DIR} && scrapy crawl {SPIDER_NAME}",
    )

# Documentation de la tâche (Optionnel, mais utile)
run_scrapy_job.doc_md = textwrap.dedent(
    """\
#### Exécution du Scrapper
Cette tâche exécute le spider Scrapy en se déplaçant d'abord dans le répertoire du projet
(`/opt/airflow/scrapers/kbo_scraper`) puis en lançant la commande `scrapy crawl`.
"""
)