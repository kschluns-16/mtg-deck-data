from bs4 import BeautifulSoup 
import requests
import pandas as pd
import re

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
            time.sleep(60*10)

    soup = BeautifulSoup(response.text,"html5lib")

    return response, soup

source = "mtgdecks.net"
output_file = "mtg_deck_data/data/mtg_decks.parquet"
base_domain = "https://mtgdecks.net"
base_url_list = [
    (f"{base_domain}/Standard/decklists/page:","Standard"),
    (f"{base_domain}/Pioneer/decklists/page:","Pioneer"),
    (f"{base_domain}/Modern/decklists/page:","Modern"),
    (f"{base_domain}/Alchemy/decklists/page:","Alchemy"),
    (f"{base_domain}/Explorer/decklists/page:","Explorer"),
    (f"{base_domain}/Historic/decklists/page:","Historic"),
]

if START_FROM_SCRATCH:
    decks_downloaded = pd.DataFrame()
    deck_urls_downloaded = set()
else:
    decks_downloaded = pd.read_parquet(output_file)
    deck_urls_downloaded = set(decks_downloaded['deck_url'].tolist())


output = []

for base_url, mtg_format in base_url_list:
        
    page_num = 1

    # UNCOMMENT BELOW IF... you have to start script from the middle of a certain page
    # page_num = 191 if mtg_format == "Modern" else 1

    next_page_available = True

    while next_page_available:

        print(f"Scraping {base_url}{page_num}")

        response, soup = safe_soup(f"{base_url}{page_num}")

        # exclude first row, since it's a header
        table_results = soup.find("table", {"class": "clickable table table-striped hidden-xs"}).find_all("tr")[1:]

        for cnt, deck_row in enumerate(table_results):
            
            deck_cols = deck_row.find_all("td")
            deck_url = base_domain + deck_cols[2].a["href"]

            if deck_url not in deck_urls_downloaded:

                deck_urls_downloaded.add(deck_url)

                response_deck, soup_deck = safe_soup(deck_url)

                regex_match = re.search("W\/L  \((\d+)\\xa0-\\xa0(\d+)\). (\d+)%",deck_cols[0].text)
                if regex_match:
                    regex_match = regex_match.groups()
                    wins = int(regex_match[0])
                    losses = int(regex_match[1])
                    wl_rate = float(regex_match[2])/100
                    place_finish = None
                else:
                    regex_match = re.search("\\n(.+)\((\d+)\\xa0-\\xa0(\d+)\). (\d+)%",deck_cols[0].text)
                    if regex_match:
                        regex_match = regex_match.groups()
                        place_finish = regex_match[0]
                        wins = int(regex_match[1])
                        losses = int(regex_match[2])
                        wl_rate = float(regex_match[3])/100
                    else:
                        wins, losses, wl_rate = None, None, None
                        place_finish = deck_cols[0].text
                

                deck_list = []
                for card_cnt, card in enumerate(soup_deck.find("textarea", {"id":"arena_deck"}).text.split("\n")):
                    card = card.split(" ",1)    
                    # card has to be length two (card count & card name)
                    if len(card) == 2:
                        deck_list += [card[1].replace(" ","")]*int(card[0])

                table = soup_deck.find("table",{"class":"clickable table table-striped hidden-xs"})

                deck_list = " ".join(deck_list) 

                output += [[mtg_format,source,deck_url,deck_list,wins,losses,wl_rate,place_finish]]

                if len(output) == 1000:

                    output = pd.DataFrame(output,columns=["mtg_format","source","deck_url","deck","wins","losses","win_rate","place"])
                    output = pd.concat([decks_downloaded,output],ignore_index=True)
                    print(f"exporting deck! ({len(output)} decks)")
                    output.to_parquet(output_file)
                    decks_downloaded = output.copy()
                    output = []

            else:
                print(f"Skipping deck {deck_url}")

        next_page_available = soup.find("ul",{"class":"pagination"}).find("li",{"class":"next disabled"}) is None
        page_num += 1

    output = pd.DataFrame(output,columns=["mtg_format","source","deck_url","deck","wins","losses","win_rate","place"])
    output = pd.concat([decks_downloaded,output],ignore_index=True)
    print(f"exporting deck! ({len(output)} decks)")
    output.to_parquet(output_file)
    decks_downloaded = output.copy()
    output = []