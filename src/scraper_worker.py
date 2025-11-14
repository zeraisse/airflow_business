import requests
from lxml import html
import pandas as pd
import time
from sqlalchemy import create_engine

# Configuration DB (Interne au docker)
DB_URI = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
engine = create_engine(DB_URI)

def get_xpath_text(tree, label_pattern):
    """Helper pour le parsing"""
    query = f"//td[contains(text(), '{label_pattern}')]/following-sibling::td[1]//text()"
    result = tree.xpath(query)
    if result:
        return " ".join([r.strip() for r in result if r.strip()])
    return None

# --- C'est cette fonction que le Daemon appelle ---
def scrape_worker(redis_client, enterprise_number, priority='low'):
    """
    Logique principale du scraping.
    """
    # 1. Gestion du proxy (via ta classe ProxyManager, ou simple request pour test)
    # Pour l'instant, on fait simple pour valider la connexion
    
    clean_num = enterprise_number.replace('.', '')
    url_kbo = f"https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={clean_num}&lang=fr"
    
    print(f"   [WORKER] Scraping de {clean_num} (Priorité: {priority})...")
    
    try:
        # Headers basiques
        headers = {'User-Agent': 'Mozilla/5.0 (Compatible; Bot/1.0)'}
        
        # Note: Ici tu devrais intégrer ton proxy_manager.get_valid_proxy()
        # resp = requests.get(url_kbo, headers=headers, proxies=..., timeout=10)
        resp = requests.get(url_kbo, headers=headers, timeout=10)

        if resp.status_code == 200:
            tree = html.fromstring(resp.content)
            
            data = {
                "enterprise_number": clean_num,
                "name": get_xpath_text(tree, 'Dénomination'),
                "status": get_xpath_text(tree, 'Statut'),
                "scraped_at": time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            print(f"   [WORKER] Succès ! Données trouvées : {data.get('name')}")
            
            # Insertion en Base de données
            pd.DataFrame([data]).to_sql('entreprises', engine, if_exists='append', index=False)
            
        else:
            print(f"   [WORKER] Erreur HTTP {resp.status_code}")

    except Exception as e:
        print(f"   [WORKER] Exception : {e}")
        # Ici tu pourrais remettre en queue si nécessaire