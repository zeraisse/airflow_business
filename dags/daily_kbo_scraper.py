import sys
import os
import pendulum
import redis
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from concurrent.futures import ThreadPoolExecutor

# On ajoute le dossier racine au path pour que Python trouve le dossier 'src'
sys.path.append('/opt/airflow')

# Assure-toi que le fichier s'appelle bien scraper_worker.py ou worker_daemon.py dans src/
# et qu'il contient la fonction scrape_worker
from src.proxy_manager import fetch_public_proxies
from src.worker_daemon import scrape_worker 

default_args = {
    'owner': 'data-team',
    'retries': 1,
}

# Fonction utilitaire pour se connecter proprement à chaque tâche
def get_redis_client():
    return redis.Redis(host='redis', port=6379, db=0)

with DAG(
    dag_id='kbo_daily_scraper',
    default_args=default_args,
    schedule='0 6 * * *', # 6h00 quotidien
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['scraping', 'kbo', 'nbb']
) as dag:

    @task
    def fetch_proxies():
        """Récupère les proxys frais et met à jour Redis"""
        fetch_public_proxies(redis_host='redis')

    @task
    def load_csv_to_queue():
        """Lit le CSV et remplit la file Redis"""
        r_client = get_redis_client()
        
        # Astuce pour trouver le CSV s'il est dans le dossier dags
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, 'enterprise.csv')
        
        try:
            df = pd.read_csv(csv_path)
            print(f"Chargement de {len(df)} entreprises depuis {csv_path}")
            
            pipeline = r_client.pipeline()
            for num in df['EnterpriseNumber']:
                # Convertir en string pour être sûr
                pipeline.lpush('queue:low', str(num))
            pipeline.execute()
            
            print("File Redis queue:low remplie.")
        except FileNotFoundError:
            print(f"ERREUR: Le fichier {csv_path} est introuvable.")
            raise

    @task
    def process_queue():
        """
        Lance les workers pour traiter la file 'low'.
        Note: Le daemon (scraper-daemon) traite aussi ces tâches en parallèle.
        Cette tâche sert ici de 'coup de boost' au moment du batch.
        """
        r_client = get_redis_client()

        def worker_wrapper(i):
            while True:
                # On écoute la file 'low' (le batch) mais aussi 'high' au cas où
                task = r_client.blpop(['queue:high', 'queue:low'], timeout=5)
                
                if not task:
                    break # Plus rien à faire pour ce thread
                
                queue_name, enterprise_num = task
                queue_name = queue_name.decode('utf-8')
                enterprise_num = enterprise_num.decode('utf-8')
                
                # Appel de la logique de scraping
                scrape_worker(r_client, enterprise_num, priority='high' if 'high' in queue_name else 'low')

        # On lance 20 threads en parallèle
        with ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(worker_wrapper, range(20))

    # Définition du flux
    proxies = fetch_proxies()
    loading = load_csv_to_queue()
    processing = process_queue()

    proxies >> loading >> processing