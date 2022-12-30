from bs4 import BeautifulSoup 
import requests
import pandas as pd
import re
from functools import reduce
from operator import add

source = "aetherhub.com"
base_domain = "https://aetherhub.com"
base_url_list = [
    (f"{base_domain}/MTGA-Decks/Standard-BO1/?p=","Standard"),
    (f"{base_domain}/MTGA-Decks/Traditional-Standard/?p=","Standard"),
    (f"{base_domain}/MTGA-Decks/Alchemy-BO1/?p=","Alchemy"),
    (f"{base_domain}/MTGA-Decks/Traditional-Alchemy/?p=","Alchemy"),
    (f"{base_domain}/MTGA-Decks/Historic-BO1/?p=","Historic"),
    (f"{base_domain}/MTGA-Decks/Traditional-Historic/?p=","Historic"),
    (f"{base_domain}/MTGA-Decks/Explorer-BO1/?p=","Explorer"),
    (f"{base_domain}/MTGA-Decks/Traditional-Explorer/?p=","Explorer"),
]
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

# decks_downloaded = pd.read_parquet("mtg_deck_data/data/aetherhub_decks.parquet")
# deck_urls_downloaded = set(decks_downloaded['deck_url'].tolist())

decks_downloaded = pd.DataFrame()
deck_urls_downloaded = set()

output = []

for base_url, mtg_format in base_url_list:

    page_num = 1
    next_page_available = True

    while next_page_available:

        print(f"Scraping {base_url}{page_num}")

        response = requests.get(f"{base_url}{page_num}", headers=headers)
        soup = BeautifulSoup(response.text,"html.parser")

        table_results = soup.find_all("tr", {"class": "ae-tbody-deckrow"})

        for cnt, deck_row in enumerate(table_results[1:]):
        
            deck_cols = deck_row.find_all("td")
            deck_url = base_domain + deck_cols[0].a["href"]

            if deck_url not in deck_urls_downloaded:

                deck_urls_downloaded.add(deck_url)

                response_deck = requests.get(deck_url,headers=headers)
                soup_deck = BeautifulSoup(response_deck.text,"html.parser")

                regex_match = re.search("(\d+)% Win Rate: (\d+) Wins - (\d+) Losses",deck_cols[3].text)
                if regex_match:
                    regex_match = regex_match.groups()
                    wl_rate = float(regex_match[0])/100
                    wins = int(regex_match[1])
                    losses = int(regex_match[2])
                    place_finish = None
                else:
                    wl_rate, wins, losses, place_finish = None, None, None, None

                tag_list = []

                for tag in soup_deck.find("div",{"class":"tab-pane fade show active"}).find_all("h5")[0].find_next_siblings():

                    if tag.name == "h5":
                        break
                    elif tag.name == "div":
                        tag_list += [tag.find_all("a",{"class":"cardLink"})]
                
                tag_list = reduce(add,tag_list)
                
                deck_list = []
                for card_cnt, card in enumerate(tag_list):
                    deck_list += [card["data-card-name"].replace(" ","")]

                deck_list = " ".join(deck_list) 

                output += [[mtg_format,source,deck_url,deck_list,wins,losses,wl_rate,place_finish]]
                    
                if len(output) == 1000:

                    output = pd.DataFrame(output,columns=["mtg_format","source","deck_url","deck","wins","losses","win_rate","place"])
                    output = pd.concat([decks_downloaded,output],ignore_index=True).drop_duplicates()
                    print(f"exporting deck! ({len(output)} decks)")
                    output.to_parquet("mtg_deck_data/data/aetherhub_decks.parquet")
                    decks_downloaded = output.copy()
                    output = []

            else:
                print(f"Skipping deck {deck_url}")

        next_page_available = (" ".join(soup.find("ul",{"class":"pagination"}).find_all("li")[-1]['class']) != "page-item disabled")
        page_num += 1

    output = pd.DataFrame(output,columns=["mtg_format","source","deck_url","deck","wins","losses","win_rate","place"])
    output = pd.concat([decks_downloaded,output],ignore_index=True).drop_duplicates()
    print(f"exporting deck! ({len(output)} decks)")
    output.to_parquet("mtg_deck_data/data/aetherhub_decks.parquet")
    decks_downloaded = output.copy()
    output = []
