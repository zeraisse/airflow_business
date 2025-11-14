# Script tourne 24h/24 pour dépiler Redis

import redis
import time
import sys
import os

# Sécurité : on s'assure que Python voit le dossier racine
sys.path.append('/opt/airflow')

# On importe la fonction de scraping
from src.scraper_worker import scrape_worker

def run_daemon():
    """Fonction principale du Daemon"""
    # Connexion Redis à l'intérieur de la fonction
    r = redis.Redis(host='redis', port=6379)
    print("Démarrage du Daemon de Scraping...")

    while True:
        try:
            # On écoute les files
            task = r.blpop(['queue:high', 'queue:low'], timeout=20)
            
            if task:
                queue_name, ent_num = task
                num = ent_num.decode('utf-8')
                q_type = queue_name.decode('utf-8')
                
                priority = 'high' if 'high' in q_type else 'low'
                print(f"Traitement de {num} depuis {q_type}")
                
                # On appelle la logique de scraping
                scrape_worker(r, num, priority=priority)
                
                # Nettoyage flag API
                r.delete(f"processing:{num}")
                
        except Exception as e:
            print(f"Erreur dans la boucle Daemon: {e}")
            time.sleep(5) # Pause sécurité en cas de crash Redis

# --- LE POINT CRUCIAL EST ICI ---
# Ce bloc signifie : "Exécute ceci SEULEMENT si je lance 'python worker_daemon.py'"
# Si Airflow fait "import worker_daemon", ce bloc est ignoré.
if __name__ == "__main__":
    run_daemon()