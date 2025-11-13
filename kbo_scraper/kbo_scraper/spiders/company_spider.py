import scrapy
import csv
from urllib.parse import urlencode
import os
import itertools

# ---
# 1. Méthode "externe" pour nettoyer le numéro
# ---
def clean_enterprise_number(raw_number: str) -> str | None:
    """
    Nettoie le numéro d'entreprise en supprimant les points.
    """
    if not raw_number:
        return None
    return raw_number.strip().replace('.', '')

# ---
# 2. Le Spider Scrapy
# ---
class CompanySpider(scrapy.Spider):
    name = "kbo_company"
    base_url = "https://kbopub.economie.fgov.be/kbopub/zoeknummerform.html"
    
    # Le chemin pointe vers le fichier à la racine du projet
    csv_input_path = 'enterprise.csv'

    async def start(self):
        """
        Cette méthode lit le CSV et génère les requêtes initiales.
        """
        self.logger.info(f"Tentative d'ouverture du CSV au chemin : {self.csv_input_path}")
        
        try:
            with open(self.csv_input_path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                # On utilise islice pour ne prendre que les 30 premières lignes
                rows_to_process = list(itertools.islice(reader, 30))

            self.logger.info(f"Fichier CSV trouvé, {len(rows_to_process)} lignes à traiter (limité aux 30 premières).")

            for row in rows_to_process:
                raw_number = row.get('EnterpriseNumber') 
                
                if raw_number is None:
                    self.logger.warning(f"La colonne 'EnterpriseNumber' n'a pas été trouvée dans une ligne. Vérifiez la casse.")
                    continue

                cleaned_number = clean_enterprise_number(raw_number)
                
                if not cleaned_number:
                    self.logger.warning(f"Numéro invalide ou manquant dans le CSV: {raw_number}")
                    continue
                
                params = {
                    'nummer': cleaned_number,
                    'actionLu': 'Rechercher'
                }
                
                url = f"{self.base_url}?{urlencode(params)}"
                
                yield scrapy.Request(
                    url=url, 
                    callback=self.parse,
                    meta={
                        'original_number': raw_number,
                        'cleaned_number': cleaned_number
                    }
                )

        except FileNotFoundError:
            self.logger.error(f"ERREUR CRITIQUE : FileNotFoundError pour le chemin : {self.csv_input_path}")
            self.logger.error("Assurez-vous que le fichier 'enterprise.csv' est bien à la racine du projet (à côté de scrapy.cfg).")
        except KeyError:
            # --- CORRECTION ICI ---
            self.logger.error(f"ERREUR CRITIQUE : La colonne 'EnterpriseNumber' n'a pas été trouvée dans le CSV.")
            self.logger.error("Vérifiez le nom exact de la colonne dans votre fichier enterprise.csv.")
        except Exception as e:
            self.logger.error(f"Une erreur inattendue est survenue lors de la lecture du CSV : {e}")


    def parse(self, response):
        """
        Cette méthode est appelée pour chaque page visitée.
        Elle scrape les données en se basant sur le HTML fourni.
        """
        self.logger.info(f"Scraping de la page pour {response.meta['cleaned_number']}")

        table_container = response.xpath('//div[@id="table"]')

        if not table_container:
            self.logger.error(f"Aucune <div id='table'> trouvée sur {response.url}")
            return

        def extract_text(label: str) -> str | None:
            raw_text = table_container.xpath(
                f'.//td[contains(text(), "{label}")]/following-sibling::td[1]//text()'
            ).getall()
            
            if not raw_text:
                return None
            
            cleaned_text = ' '.join([text.strip() for text in raw_text if text.strip()])
            return cleaned_text.replace('\xa0', ' ')

        def extract_full_text(label: str) -> str | None:
            node = table_container.xpath(
                f'.//td[contains(text(), "{label}")]/following-sibling::td[1]'
            )
            if not node:
                return None
            
            all_text_nodes = node.xpath('.//text()').getall()
            cleaned_text = ' '.join([text.strip() for text in all_text_nodes if text.strip()])
            return cleaned_text.replace('\xa0', ' ')

        # ---
        # Extraction des données
        # ---
        yield {
            'numero_demande': response.meta['cleaned_number'],
            'numero_bce': extract_text("Numéro d'entreprise:"),
            'statut': extract_text("Statut:"),
            'situation_juridique': extract_full_text("Situation juridique:"),
            'date_debut': extract_text("Date de début:"),
            'denomination': extract_full_text("Dénomination:"),
            'adresse_siege': extract_full_text("Adresse du siège:"),
            'forme_legale': extract_full_text("Forme légale:"),
            'source_url': response.url,
        }