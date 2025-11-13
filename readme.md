# Lancer scraper enterprise : 

mettre enterprise.csv dans le dossier kbo_scraper

# cmd
```bash
scrapy startproject kbo_scraper
cd kbo_scraper
scrapy crawl kbo_company -o ./sprapping/output.json
```