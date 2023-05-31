import json
import pandas as pd
from typing import List
from geopy.geocoders import Nominatim

# load info
def get_info() -> List:
    # load json for info
    json_file = "info.json"
    with open(json_file) as f:
        infos = json.load(f)
    return infos["sources"]


# get state by co-ordinate
def get_state_by_coord(coord):
    try:
        geolocator = Nominatim(user_agent="geoapi")
        location = geolocator.reverse(coord, exactly_one=True)
        address = location.raw['address']
        state = address.get('state', '')
        return state
    except (AttributeError, KeyError, ValueError, ):
        return None

# make absolute url
def get_kba_abs_urls(url_str: str) -> str:
    fixed_dates = {"2023 4":("28_2023_04", "2"), "2023 3":("28_2023_03", "6"), "2023 2":("28_2023_02", "6"), "2023 1": ("28_2023_01", "6"), "2022 12": ("28_2022_12", "6"), "2022 11": ("28_2022_11", "7"), 
                   "2022 10": ("28_2022_10", "4"), "2022 9": ("28_2022_09", "4"), "2022 8": ("28_2022_08", "5"), "2022 7":("28_2022_07", "6"), "2022 6": ("28_2022_06", "8")}
    
    urls = []
    for _, value in fixed_dates.items():
        urls.append(url_str.format(value[0], value[1]))
        
    return urls

# download and save in memory excel file
def get_kba_excel(url: str, sheet_name: str):
    urls = get_kba_abs_urls(url)
    kba_data = pd.DataFrame()
    for url in urls:
        df = pd.read_excel(url, sheet_name=sheet_name, usecols="B:C")
        df.rename(columns={"Unnamed: 1": "State", "Unnamed: 2": "EV Sales"}, inplace=True)
        df = df.iloc[12:28]
        
        # check if kba_data is empty
        if kba_data.empty:
            kba_data = df
        else: # otherwise add later values
            for ind in df.index:
                index = kba_data.index[df["State"][ind]==kba_data["State"]].to_list()[0]
                kba_data["EV Sales"][index] += df["EV Sales"][ind]
                
    return kba_data


if __name__ == "__main__":
    # get_state_by_coord("52.28400703, 11.42978302")
    url = get_info()[1]["absolute_url"]
    get_kba_excel(url, "FZ 28.9")
    pass