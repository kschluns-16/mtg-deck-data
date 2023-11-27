from bs4 import BeautifulSoup
from selenium import webdriver
import requests
import pandas as pd
import re
import numpy as np

START_FROM_SCRATCH = False

headers = {
    # "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}


def safe_soup(url):
    is_response_done = False
    while not is_response_done:
        try:
            # response = requests.get(url, headers=headers)
            response = webdriver.Chrome()
            response.get(url)
            is_response_done = True
        except:
            print(
                f"failed getting response from {url} (waiting 10 min before the next call)"
            )
            time.sleep(60 * 10)

    # soup = BeautifulSoup(response.text, "html5lib")
    soup = BeautifulSoup(response.page_source, "html5lib")

    return response, soup


source = "mtgdecks.net"
output_file = "mtg_deck_data/data/mtg_decks.parquet"
base_domain = "https://mtgdecks.net"
base_url = f"{base_domain}/Standard/staples/page:"

if START_FROM_SCRATCH:
    output = []

    page_num = 0

    page_available = True

    while page_available:
        print(f"Scraping {base_url}{page_num}")

        response, soup = safe_soup(f"{base_url}{page_num}")

        if page_num == 0:
            soup_search_str = "col-md-3 col-lg-2 col-sm-5ths col-xs-6"
        else:
            soup_search_str = "col-md-3 col-lg-2 col-sm-4 col-xs-6"

        table_results = soup.find_all("div", {"class": soup_search_str})

        if len(table_results) > 0:
            page_available = True
        else:
            page_available = False

        for cnt, card_row in enumerate(table_results):
            card_name = card_row.find("b", {"class": "text-center"}).text
            card_stats = card_row.find_all(
                "div", {"class": "btn btn-default btn-sm col-xs-4"}
            )
            card_popularity, card_copies_per_deck, card_num_decks = [
                stat.find("b").string for stat in card_stats
            ]

            output += [
                [
                    card_name,
                    card_popularity,
                    card_copies_per_deck,
                    card_num_decks,
                ]
            ]

        page_num += 1

    output = pd.DataFrame(
        output,
        columns=[
            "name",
            "popularity",
            "copies_per_deck",
            "num_decks",
        ],
    ).sort_values("popularity", ascending=False)
    output["popularity"] = (
        output["popularity"].str.replace("%", "").astype("float") / 100.0
    )
    output["copies_per_deck"] = output["copies_per_deck"].astype("float")
    output["num_decks"] = output["num_decks"].astype("float")
    output["copies_per_deck_cumsum"] = output["copies_per_deck"].cumsum()
    output["total_cards"] = output.eval("copies_per_deck * num_decks")

    output.to_csv("./mtg_deck_data/data/mtg_decks_top_cards.csv", index=False)
else:
    output = pd.read_csv("./mtg_deck_data/data/mtg_decks_top_cards.csv")


def prepare_card_set(
    input_df=pd.DataFrame,
    num_base_cards=100,
    total_cards_min_printed=1,
    total_cards_max_printed=8,
    print_card_list=False,
):
    df = (
        input_df.copy()
        .sort_values("total_cards", ascending=False)
        .iloc[:num_base_cards]
    )

    total_cards_min = df["total_cards"].min()
    total_cards_max = df["total_cards"].max()

    df["total_cards_scaled"] = (df["total_cards"] - total_cards_min) / (
        total_cards_max - total_cards_min
    )

    df["total_cards_printed_ceil"] = total_cards_min_printed + (
        (total_cards_max_printed - total_cards_min_printed) * df["total_cards_scaled"]
    )

    df["total_cards_printed"] = np.round(
        df[["copies_per_deck", "total_cards_printed_ceil"]].max(axis=1)
    ).astype("int")

    if print_card_list:
        for _, row in df.iterrows():
            print(f"{row['total_cards_printed']}x {row['name']}")

    return df


# play with the function inputs below
output_selection = prepare_card_set(
    input_df=pd.DataFrame,
    num_base_cards=189,
    total_cards_min_printed=1,
    total_cards_max_printed=8,
    print_card_list=True,
)
