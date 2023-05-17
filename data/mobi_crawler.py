# crawler for extracting data from https://mobilithek.info/offers/-2413665570381145802
# data source :: https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebieten/Energie/Unternahmen_Institutionen/E_Mobilitaet/Ladesaeulenregister.xlsx%3f__blob%3dpublicationFile%26v%3d21

import json
import requests as req
import datetime
import io
import csv

def crawl():
    # load json for info
    json_file = "info.json"
    with open(json_file) as f:
        infos = json.load(f)
    # extract mobi infos
    mobi_infos = infos["sources"][0]
    print(mobi_infos["absolute_url"])
    # download csv data into files
    # Send a GET request to the URL
    #response = req.get(mobi_infos["absolute_url"])
    
    response = req.get(mobi_infos["absolute_url"])
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        
        # Save the response content to a file
        current_time = datetime.datetime.now()
        # Format the timestamp as a string
        timestamp = current_time.strftime("%Y%m%d%H%M%S")
        file_name = f"mobilitek_data_{timestamp}.csv"
        
        # Create a file-like object from the response content
        file_obj = io.StringIO(response.content.decode('utf-8'))
        reader = csv.reader(file_obj, delimiter=";")
        
        with open(f"files/{file_name}", 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(reader)
            
        print("File downloaded successfully.")
    else:
        print("Failed to download the file.")
    

if __name__ == '__main__':
    crawl()