import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime
from database import upload_to_mongo

def scrape_countries_data():
    try:
        print("calling from airfow") 
        URL = "https://www.scrapethissite.com/pages/simple/"
        webpage = requests.get(URL)
        soup = BeautifulSoup(webpage.content, "html.parser")
        country_names = soup.find_all("h3", attrs={'class':'country-name'})
        country_capitals = soup.find_all("span", attrs={'class':'country-capital'})
        country_populations = soup.find_all("span", attrs={'class':'country-population'})
        country_areas = soup.find_all("span", attrs={'class':'country-area'})

        data =[]
        for country_name, country_capital, country_population, country_area in zip(country_names, country_capitals, country_populations, country_areas):
            data.append({
            "name": country_name.get_text(strip=True),
            "capital": country_capital.get_text(strip=True),
            "population": country_population.get_text(strip=True),
            "area": country_area.get_text(strip=True),
            "scraped_datetime":datetime.now()
        })
        df = pd.DataFrame(data)
        data_dir = os.path.join(os.getcwd(), 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        # Save the CSV file to the data directory
        file_name = 'countries_data.csv'
        csv_file = os.path.join(data_dir,file_name)
        df.to_csv(csv_file, index=False)
        upload_to_mongo(file_name=file_name,collection_name='countries')
        print("called")
    except Exception as e:
        print(f"error {e}")



def scrape_hockey_teams_data():
    try:
    # sample="https://www.scrapethissite.com/pages/forms/?page_num=1&per_page=100"
        base_url = "https://www.scrapethissite.com/pages/forms/?page_num="
        data = []
        for i in range(1,5):
            # print(i)
            url = f"{base_url}{i}&per_page=100"
            # print(url)
            webpage = requests.get(url)
            soup = BeautifulSoup(webpage.content, "html.parser")
            hockey_teams = soup.find_all("td", attrs={'class':'name'})
            hockey_years = soup.find_all("td", attrs={'class':'year'})
            hockey_wins = soup.find_all("td", attrs={'class':'wins'})
            hockey_losses = soup.find_all("td", attrs={'class':'losses'})
            hockey_ot_losses = soup.find_all("td", attrs={'class':'ot-losses'})
            hockey_pct = soup.find_all("td", attrs={'class':'pct'})
            hockey_gf = soup.find_all("td", attrs={'class':'gf'})
            hockey_ga = soup.find_all("td", attrs={'class':'ga'})
            hockey_diff = soup.find_all("td", attrs={'class':'diff'})
            
            
            for team, year, wins,losses, ot_losses,pct,gf,ga,diff in zip(hockey_teams,hockey_years,hockey_wins, hockey_losses,hockey_ot_losses, hockey_pct,hockey_gf,hockey_ga,hockey_diff):
                data.append({
                    "team":team.get_text(strip=True),
                    "year":year.get_text(strip=True),
                    "wins":wins.get_text(strip=True),
                    "losses":losses.get_text(strip=True),
                    "ot_losses":ot_losses.get_text(strip=True),
                    "pct":pct.get_text(strip=True),
                    "gf":gf.get_text(strip=True),
                    "ga":ga.get_text(strip=True),
                    "diff":diff.get_text(strip=True),
                    "scraped_datetime":datetime.now()
                })
        # print(data)
        df = pd.DataFrame(data)
        data_dir = os.path.join(os.getcwd(), 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        # Save the CSV file to the data directory
        file_name = 'hockey_teams.csv'
        csv_file = os.path.join(data_dir,file_name)
        df.to_csv(csv_file, index=False)

        # insert data to mongodb
        # upload_to_mongo(file_name=file_name,collection_name='hockey_teams')
    except Exception as e:
        print(f"error {e}")

# scrape_hockey_teams_data()
# scrape_countries_data()
upload_to_mongo('countries_data.csv','countries')