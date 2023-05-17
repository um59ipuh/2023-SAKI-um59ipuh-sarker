# here we load the data and then decide which data we are going to use

import io
import os
import pandas as pd
from geopy.geocoders import Nominatim


def get_state_by_location(latitude, longitude):
    geolocator = Nominatim(user_agent='ms-project') 

    try:
        # Geocode the location
        location_data = geolocator.reverse(f"{latitude}, {longitude}", exactly_one=True)

        if location_data:
            # Extract the state information
            state = location_data.raw['address'].get('state')

            if state:
                return state
            else:
                return 'State information not found'
        else:
            return 'Location not found'

    except Exception as e:
        print(f'An error occurred: {str(e)}')
        return None

# mobilithek data
def transform_mobilithek():
    # load dataset from files
    file_path = os.path.join("files", "mobilitek_data_20230517152107.csv")
    df = pd.read_csv(file_path, on_bad_lines='skip')
    # extract only needed data from whole file
    selected_columns = ["anzahl_ladepunkte", "koordinaten"]
    df = df[selected_columns]
    # df['state'] = df.apply(lambda row: get_state_by_location(row['koordinaten'].split(",")[0], row['koordinaten'].split(",")[1]), axis=1)
    # save temporary to memory
    return df
    


def transform_kba():
    # load dataset from files
    file_path = os.path.join("files", "merged_data.csv")
    # merge all data into one file
    df = pd.read_csv(file_path, on_bad_lines='skip')
    # save temporary to memory
    return df


if __name__ == "__main__":
    #print(get_state_by_location("47.681046", "9.823813"))
    transform_mobilithek()