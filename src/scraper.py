# Le code qui scrape KBO (Requests ou Scrapy)

import requests
from lxml import html
import pandas as pd
import pdfplumber
import io
import pydoop.hdfs as hdfs
from sqlalchemy import create_engine, text

# Configuration DB
DB_URI = "postgresql+psycopg2://user:pass@host:5432/scraping_db"
engine = create_engine(DB_URI)

def scrape_worker(redis_client, enterprise_number, priority='low'):
    proxy_manager = RedisProxyManager(redis_client)
    
    max_retries = 3
    attempt = 0
    
    clean_num = enterprise_number.replace('.', '')
    url_kbo = f"https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={clean_num}&lang=fr"
    
    while attempt < max_retries:
        proxy = None
        try:
            proxy = proxy_manager.get_valid_proxy()
            proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
            
            print(f"Scraping {clean_num} avec {proxy} (Tentative {attempt+1})")
            
            # Timeout strict pour éviter de bloquer le worker
            resp = requests.get(url_kbo, proxies=proxies_dict, timeout=10, headers={'User-Agent': 'Mozilla/5.0...'})
            
            # Gestion des erreurs HTTP
            if resp.status_code == 404:
                print(f"404 pour {clean_num} - Ne pas stocker.")
                return # On arrête, pas de retry
            
            if resp.status_code in [403, 429, 500, 502, 503]:
                raise requests.exceptions.RequestException("Blocage ou erreur serveur")

            # --- PARSING (Code précédent adapté) ---
            tree = html.fromstring(resp.content)
            data = {
                "enterprise_number": clean_num,
                "name": tree.xpath("//td[contains(text(), 'Dénomination')]/following-sibling::td[1]//text()"),
                # ... autres champs ...
                "raw_html": "..." # NON, consigne 6: ne pas stocker si erreur, mais ici c'est succès
            }
            
            # --- ECRITURE DB ---
            # Insérer data dans PostgreSQL
            pd.DataFrame([data]).to_sql('entreprises', engine, if_exists='append', index=False)
            
            # --- GESTION PDF / HDFS ---
            # Appel hypothétique pour le PDF
            # save_pdf_to_hdfs(clean_num, proxies_dict) 
            
            proxy_manager.report_success(proxy)
            return # Succès, on quitte

        except Exception as e:
            print(f"Echec pour {clean_num} avec {proxy}: {e}")
            if proxy:
                proxy_manager.report_failure(proxy)
            attempt += 1
            
    # Si on arrive ici, c'est un échec définitif après 3 essais
    # Remise en file d'attente priorité basse (Consigne 4)
    if priority == 'high':
         print(f"Réinsertion de {clean_num} en file basse priorité.")
         # redis_client.rpush('queue:low', enterprise_number) # A implémenter
    else:
        print(f"Abandon pour {clean_num} après trop d'échecs.")