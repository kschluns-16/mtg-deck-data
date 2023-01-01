from bs4 import BeautifulSoup 
import requests
import pandas as pd
import time

START_FROM_SCRATCH = False

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

def safe_soup(url):

    is_response_done = False 
    while not is_response_done:
        try:
            response = requests.get(url, headers=headers)
            is_response_done = True
        except:
            print(f"failed getting response from {url} (waiting 10 min before the next call)")
            time.wait(60*10)

    soup = BeautifulSoup(response.text,"html5lib")

    return response, soup

source = "aetherhub.com"
output_file = "mtg_deck_data/data/mtga_user_decks.parquet"
base_domain = "https://aetherhub.com"
base_url_list = [
    f"{base_domain}/MTGA/ConstructedRankingLadder/48?p=",
]

if START_FROM_SCRATCH:
    decks_downloaded = pd.DataFrame()
    deck_urls_downloaded = set()
else:
    decks_downloaded = pd.read_parquet(output_file)
    deck_urls_downloaded = set(decks_downloaded['deck_url'].tolist())

output = []

for base_url in base_url_list:

    page_num = 1
    next_page_available = True

    while next_page_available:

        print(f"Scraping {base_url}{page_num}")

        response, soup = safe_soup(f"{base_url}{page_num}")

        user_list = soup.find_all("td", {"class": "rank-user"})

        # UNCOMMENT BELOW IF... you have to start script from the middle of a certain page
        # user_list = user_list[79:] if page_num == 11 else user_list

        for user in user_list:
            
            user_url = user.find_all("a")[-1]['href']
            print(f"Scraping {user_url}")

            place_finish = user.find("span").text.strip(" \r\n").replace("\r","").replace("\n","")

            response_user, soup_user = safe_soup(user_url)

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
                wl_rate = wins / (wins + losses) if (wins + losses) > 0 else None

                if deck_url not in deck_urls_downloaded:

                    deck_urls_downloaded.add(deck_url)

                    response_deck, soup_deck = safe_soup(deck_url)
                    
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
                        output.to_parquet(output_file)
                        decks_downloaded = output.copy()
                        output = []

                else:
                    print(f"Skipping deck {deck_url}")
            
        next_page_available = (" ".join(soup.find("ul",{"class":"pagination"}).find_all("li")[-1]['class']) != "page-item disabled")
        page_num += 1

    output = pd.DataFrame(output,columns=["mtg_format","source","deck_url","deck","wins","losses","win_rate","place"])
    output = pd.concat([decks_downloaded,output],ignore_index=True).drop_duplicates()
    print(f"exporting deck! ({len(output)} decks)")
    output.to_parquet(output_file)
    decks_downloaded = output.copy()
    output = []