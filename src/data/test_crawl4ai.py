import asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from bs4 import BeautifulSoup
import json
import pandas as pd
from pathlib import Path

async def scrape_strava_leaderboard(segment_url, debug=True):
    """
    Scrape le leaderboard d'un segment Strava
    
    Args:
        segment_url: URL du segment Strava
        debug: Si True, sauvegarde le HTML pour inspection
    
    Returns:
        Liste de dictionnaires contenant les données du leaderboard
    """
    
    browser_config = BrowserConfig(
        headless=False,  # Mode visible pour debug
        verbose=True
    )
    
    crawler_config = CrawlerRunConfig(
        # Attendre plus longtemps que le tableau se charge
        wait_for="css:table tbody tr",
        page_timeout=30000,
        delay_before_return_html=5.0,  # Attendre 5 secondes
        
        # JavaScript pour scroller et attendre
        js_code=[
            "await new Promise(r => setTimeout(r, 3000));",  # Attendre 3s
            "window.scrollTo(0, 500);",  # Scroller un peu
            "await new Promise(r => setTimeout(r, 2000));"   # Attendre encore 2s
        ]
    )
    
    leaderboard_data = []
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        print("🌐 Chargement de la page...")
        result = await crawler.arun(
            url=segment_url,
            config=crawler_config
        )
        
        # Sauvegarder le HTML pour debug
        if debug:
            debug_path = Path('debug_strava.html')
            try:
                with open(debug_path, 'w', encoding='utf-8') as f:
                    f.write(result.html)
                print(f"🔍 HTML sauvegardé dans '{debug_path.absolute()}'")
            except Exception as e:
                print(f"⚠️ Erreur sauvegarde HTML: {e}")
        
        # Parser le HTML
        soup = BeautifulSoup(result.html, 'html.parser')
        
        # Chercher TOUS les tableaux
        all_tables = soup.find_all('table')
        print(f"\n📊 {len(all_tables)} tableau(x) trouvé(s) dans la page")
        
        if not all_tables:
            print("❌ Aucun tableau trouvé!")
            return []
        
        # Analyser chaque tableau
        for table_idx, table in enumerate(all_tables):
            print(f"\n🔍 Analyse du tableau {table_idx + 1}:")
            
            tbody = table.find('tbody')
            if not tbody:
                print("   ⚠️ Pas de tbody")
                continue
            
            rows = tbody.find_all('tr')
            print(f"   📋 {len(rows)} ligne(s)")
            
            if len(rows) == 0:
                continue
            
            # Analyser la première ligne pour comprendre la structure
            first_row = rows[0]
            cells = first_row.find_all(['td', 'th'])
            print(f"   📏 {len(cells)} cellule(s) par ligne")
            
            # Afficher le contenu des cellules pour debug
            print(f"\n   📝 Contenu de la première ligne:")
            for i, cell in enumerate(cells):
                text = cell.get_text(strip=True)[:60]
                print(f"      Cellule {i}: '{text}'")
            
            # Si c'est le bon tableau (contient "time" dans une cellule)
            has_time = any('time' in cell.get_text().lower() or ':' in cell.get_text() 
                          for cell in cells)
            
            if not has_time and len(rows) > 1:
                # Vérifier la deuxième ligne aussi
                second_row_cells = rows[1].find_all(['td', 'th'])
                has_time = any('time' in cell.get_text().lower() or ':' in cell.get_text() 
                              for cell in second_row_cells)
            
            if not has_time:
                print(f"   ⚠️ Pas de données temporelles, probablement pas le leaderboard")
                continue
            
            print(f"\n   ✅ Ce tableau semble être le leaderboard!")
            
            # Parser toutes les lignes
            for idx, row in enumerate(rows):
                try:
                    cells = row.find_all('td')
                    
                    if len(cells) == 0:
                        continue
                    
                    # Extraire toutes les cellules
                    data_cells = [cell.get_text(strip=True) for cell in cells]
                    
                    # Chercher la cellule avec le temps (format MM:SS ou H:MM:SS)
                    time_value = None
                    time_idx = None
                    for i, cell_text in enumerate(data_cells):
                        if ':' in cell_text and len(cell_text) < 10:
                            time_value = cell_text
                            time_idx = i
                            break
                    
                    if not time_value:
                        continue
                    
                    # Extraire le nom de l'athlète (généralement dans un lien)
                    athlete_name = "Unknown"
                    for cell in cells:
                        link = cell.find('a')
                        if link and '/athletes/' in link.get('href', ''):
                            athlete_name = link.get_text(strip=True)
                            break
                    
                    entry = {
                        'rank': idx + 1,
                        'athlete_name': athlete_name,
                        'time': time_value,
                        'raw_data': data_cells  # Garder toutes les données
                    }
                    
                    leaderboard_data.append(entry)
                    
                except Exception as e:
                    print(f"   ⚠️ Erreur ligne {idx+1}: {e}")
                    continue
            
            if leaderboard_data:
                break  # On a trouvé le bon tableau
        
        print(f"\n✅ {len(leaderboard_data)} entrée(s) extraite(s)")
        
    return leaderboard_data


async def main():
    segment_url = "https://www.strava.com/segments/22495117"
    
    print(f"🚀 Scraping du leaderboard: {segment_url}\n")
    
    data = await scrape_strava_leaderboard(segment_url, debug=True)
    
    if data:
        print("\n📊 Résultats du leaderboard:\n")
        for entry in data[:10]:
            print(f"#{entry['rank']} - {entry['athlete_name']} - {entry['time']}")
        
        # Sauvegarder
        with open('strava_leaderboard.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print("\n💾 Données sauvegardées dans 'strava_leaderboard.json'")
        
        df = pd.DataFrame(data)
        df.to_csv('strava_leaderboard.csv', index=False, encoding='utf-8')
        print("💾 Données sauvegardées dans 'strava_leaderboard.csv'")
        
        print(f"\n📈 Total: {len(data)} entrées")
    else:
        print("\n❌ Aucune donnée extraite")
        print("\n💡 Vérifiez le fichier 'debug_strava.html'")
        print("   Si le fichier est vide ou manquant, le problème vient du chargement de la page")


if __name__ == "__main__":
    asyncio.run(main())