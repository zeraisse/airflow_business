# Lancer scraper enterprise : 

mettre enterprise.csv dans le dossier kbo_scraper

# cmd
```bash
docker network create business-net
cd kbo_scraper
scrapy crawl kbo_company -o ./sprapping/output.json
```