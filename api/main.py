from fastapi import FastAPI, HTTPException, BackgroundTasks
import redis
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import time

app = FastAPI()

# Configuration (à mettre dans un .env)
REDIS_HOST = 'redis'
DB_URI = "postgresql+psycopg2://user:pass@host:5432/scraping_db"

r_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)
engine = create_engine(DB_URI)

def check_database(clean_num):
    """Vérifie si l'entreprise est déjà en base et scannée récemment (< 7 jours)"""
    query = text("""
        SELECT * FROM entreprises 
        WHERE enterprise_number = :num 
        AND last_updated > NOW() - INTERVAL '7 days'
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"num": clean_num}).fetchone()
        return result

@app.get("/search/{enterprise_number}")
async def search_company(enterprise_number: str):
    """
    Endpoint appelé par ton formulaire.
    """
    clean_num = enterprise_number.replace('.', '').strip()
    
    # 1. Vérification Cache / Base de données
    existing_data = check_database(clean_num)
    if existing_data:
        return {"status": "found", "source": "database", "data": dict(existing_data._mapping)}

    # 2. Si pas en base, on lance le scraping
    # On vérifie si le job n'est pas déjà en cours pour éviter les doublons
    if r_client.get(f"processing:{clean_num}"):
        return {"status": "pending", "message": "Scraping déjà en cours, veuillez patienter."}

    # 3. Ajout à la file prioritaire (High Priority)
    print(f"Demande utilisateur reçue pour {clean_num} -> Push queue:high")
    
    # On push à gauche (LPUSH) pour que ce soit traité en premier par le worker
    r_client.lpush('queue:high', clean_num)
    
    # On pose un marqueur temporaire pour dire "c'est en cours"
    r_client.setex(f"processing:{clean_num}", 300, "1") # Expire dans 5 min

    return {
        "status": "accepted", 
        "source": "scraper", 
        "message": "Recherche lancée. Veuillez réactualiser dans quelques secondes.",
        "poll_url": f"/check-status/{clean_num}"
    }

@app.get("/check-status/{enterprise_number}")
def check_status(enterprise_number: str):
    """Endpoint pour le frontend qui 'poll' (vérifie) si c'est fini"""
    clean_num = enterprise_number.replace('.', '').strip()
    
    data = check_database(clean_num)
    if data:
         return {"status": "done", "data": dict(data._mapping)}
    
    return {"status": "pending"}