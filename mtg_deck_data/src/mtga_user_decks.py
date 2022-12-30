from bs4 import BeautifulSoup 
import requests
import pandas as pd
import re
from functools import reduce
from operator import add

source = "aetherhub.com"
base_domain = "https://aetherhub.com"
base_url_list = [
    f"{base_domain}/MTGA/ConstructedRankingLadder/?p=",
]
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

decks_downloaded = pd.read_parquet("mtg_deck_data/data/mtga_user_decks.parquet")
deck_urls_downloaded = set(decks_downloaded['deck_url'].tolist())

# decks_downloaded = pd.DataFrame()
# deck_urls_downloaded = set()

output = []

for base_url in base_url_list:

    page_num = 1
    next_page_available = True

    while next_page_available:

        print(f"Scraping {base_url}{page_num}")

        response = requests.get(f"{base_url}{page_num}", headers=headers)
        soup = BeautifulSoup(response.text,"html.parser")

        user_list = soup.find_all("td", {"class": "rank-user"})

        for user in user_list:
            
            user_url = user.find_all("a")[-1]['href']
            print(f"Scraping {user_url}")

            place_finish = user.find("span").text.strip(" \r\n").replace("\r","").replace("\n","")

            response_user = requests.get(user_url, headers=headers)
            soup_user = BeautifulSoup(response_user.text,"html.parser")

            table_results = soup_user.find_all("div", {"class":"d-flex justify-content-start flex-wrap mb-3"})

            for cnt, deck_row in enumerate(table_results):
            
                deck_details = deck_row.find("header",{"class":"p-2"})
                deck_url = "https://mtgaassistant.net" + deck_details.a['href']
                mtg_format = deck_details.div.text

                event_stats = deck_row.find_all("div",{"class":"acsgroup badge text-center mr-2 mb-1 deckstats"}) 
                wins = 0
                losses = 0
                for event in event_stats:
                    event = event.find_all("div")
                    wins += int(event[0].text.split(" ")[0])
                    losses += int(event[1].text.split(" ")[0])
                wl_rate = wins / (wins + losses)

                if deck_url not in deck_urls_downloaded:

                    deck_urls_downloaded.add(deck_url)

                    response_deck = requests.get(deck_url,headers=headers)
                    soup_deck = BeautifulSoup(response_deck.text,"html.parser")
                    
                    deck_table = soup_deck.find("div",{"class":"deck-container mb-3"})

                    if deck_table:
                            
                        deck_list = []
                        for card in deck_table.find_all("div",{"class":"ae-card-row d-flex"}):
                            deck_list += [card.find("div",{"class":"flex-grow-1 displayed-name"}).text.replace(" ","")]*int(card.find("div",{"class":"ae-quantity"}).text)

                        deck_list = " ".join(deck_list) 

                        output += [[mtg_format,source,deck_url,deck_list,wins,losses,wl_rate,place_finish]]
                        
                    if len(output) == 1000:

                        output = pd.DataFrame(output,columns=["mtg_format","source","deck_url","deck","wins","losses","win_rate","place"])
                        output = pd.concat([decks_downloaded,output],ignore_index=True).drop_duplicates()
                        print(f"exporting deck! ({len(output)} decks)")
                        output.to_parquet("mtg_deck_data/data/mtga_user_decks.parquet")
                        decks_downloaded = output.copy()
                        output = []

                else:
                    print(f"Skipping deck {deck_url}")
            
        next_page_available = (" ".join(soup.find("ul",{"class":"pagination"}).find_all("li")[-1]['class']) != "page-item disabled")
        page_num += 1

    output = pd.DataFrame(output,columns=["mtg_format","source","deck_url","deck","wins","losses","win_rate","place"])
    output = pd.concat([decks_downloaded,output],ignore_index=True).drop_duplicates()
    print(f"exporting deck! ({len(output)} decks)")
    output.to_parquet("mtg_deck_data/data/mtga_user_decks.parquet")
    decks_downloaded = output.copy()
    output = []
