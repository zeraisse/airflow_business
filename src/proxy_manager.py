import redis
import time
import requests

class RedisProxyManager:
    def __init__(self, redis_client):
        self.r = redis_client
        self.PROXY_KEY = "proxies:list"
        self.COOLDOWN_KEY = "proxies:cooldown" # Hash map IP -> Timestamp fin cooldown
        self.USAGE_KEY = "proxies:last_use"    # Hash map IP -> Timestamp dernière utilisation
        
    def load_proxies(self, proxy_list):
        """Charge les proxys depuis une liste"""
        # Nettoyer l'existant si nécessaire
        self.r.delete(self.PROXY_KEY)
        for p in proxy_list:
            self.r.lpush(self.PROXY_KEY, p)

    def get_valid_proxy(self):
        """
        Récupère un proxy qui respecte:
        - Pas en cooldown (échec précédent)
        - Pas utilisé dans les 20 dernières secondes
        """
        # Tentative simple (pour éviter boucle infinie en prod, mettre une limite)
        for _ in range(100):
            # Rotation simple (Round Robin)
            proxy = self.r.rpoplpush(self.PROXY_KEY, self.PROXY_KEY)
            if not proxy:
                raise Exception("Aucun proxy disponible dans Redis")
            
            proxy = proxy.decode('utf-8')
            current_time = time.time()
            
            # Vérifier le Cooldown
            cooldown_end = self.r.hget(self.COOLDOWN_KEY, proxy)
            if cooldown_end and current_time < float(cooldown_end):
                continue # Proxy puni
                
            # Vérifier le Rate Limit
            last_use = self.r.hget(self.USAGE_KEY, proxy)
            if last_use and (current_time - float(last_use)) < 20:
                continue # Trop tôt pour ce proxy

            # Si on arrive ici, le proxy est bon. On marque son utilisation.
            self.r.hset(self.USAGE_KEY, proxy, current_time)
            return proxy
            
        raise Exception("Aucun proxy valide trouvé après 100 tentatives (tous en cooldown ?)")

    def report_failure(self, proxy):
        """Marque un proxy en erreur (5 min cooldown)"""
        cooldown_duration = 300 # 5 minutes
        future_time = time.time() + cooldown_duration
        self.r.hset(self.COOLDOWN_KEY, proxy, future_time)
        print(f"Proxy {proxy} mis en quarantaine pour 5 min.")

    def report_success(self, proxy):
        pass

# --- FONCTION UTILISÉE PAR LE DAG ---

def fetch_public_proxies(redis_host='redis'):
    """Récupère des proxys gratuits et remplit Redis"""
    print("Récupération des proxys publics...")
    
    # Source API gratuite (Proxyscrape)
    url = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            proxies = response.text.strip().split('\n')
            proxies = [p.strip() for p in proxies if p.strip()]
            
            r = redis.Redis(host=redis_host, port=6379, db=0)
            
            # On vide la vieille liste
            r.delete("proxies:list")
            
            # On remplit la nouvelle
            if proxies:
                r.lpush("proxies:list", *proxies)
            
            print(f"Succès : {len(proxies)} proxys ajoutés à Redis.")
        else:
            print("Erreur lors du téléchargement de la liste.")
            
    except Exception as e:
        print(f"Erreur critique fetch_proxies: {e}")