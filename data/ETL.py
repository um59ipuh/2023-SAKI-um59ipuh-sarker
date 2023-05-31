import pandas as pd
import ETL_services as service
import os.path as path

# ----------- E --------------
# mobi data
def extract_mobi_data():
    url = service.get_info()[0]["absolute_url"]
    file_path = "./mobi-data-raw.csv"
    mobi_data = pd.DataFrame()
    if not path.isfile(file_path):
        mobi_data = pd.read_csv(url, delimiter=";")
        mobi_data.to_csv(file_path, index=False)
    else:
        mobi_data = pd.read_csv(file_path)
    return mobi_data

# kba data
def extract_kba_data():
    info = service.get_info()[1]
    # paramerized url for kba
    url = info["absolute_url"]
    # intended sheet name from excelfile
    sheet = info["sheet_name"]
    file_path = "./kba-data-raw.csv"
    kba_data = pd.DataFrame()
    if not path.isfile(file_path):
        kba_data = service.get_kba_excel(url, sheet)
        kba_data.to_csv(file_path, index=False)
    else:
        kba_data = pd.read_csv(file_path)
    return kba_data

# ----------- T --------------
# transform data
def transform_to_table(mobi_data, kba_data):
    # ----------- tansform mobi data --------------
    # select specific column from mobi data
    selected_columns = ["anzahl_ladepunkte", "anschlussleistung", "koordinaten"]
    mobi_data = mobi_data[selected_columns].rename(columns={"anzahl_ladepunkte": "number_of_charging_point", "anschlussleistung" : "connected_load", 
                                          "koordinaten" : "coordinates"})
    # add column state in place of coordinate
    # check if the file already exist or not
    generated_file_path = "./mobi-data.csv"
    if not path.isfile(generated_file_path):
        # select a fraction of data from dataset
        mobi_data = mobi_data.iloc[:5000]
        # generate the file
        mobi_data["coordinates"] = mobi_data["coordinates"].map(service.get_state_by_coord, na_action='ignore')
        mobi_data.rename(columns={"coordinates" : "State"}, inplace=True)
        mobi_data.to_csv(generated_file_path, index=False)
        
    # if exist then read it and make actual one
    mobi_data = pd.read_csv(generated_file_path)
    mobi_data = mobi_data.groupby(["State"]).agg({"number_of_charging_point": sum, "connected_load": sum}).reset_index()
    de_states = ["Baden-Württemberg", "Bayern", "Brandenburg", "Bremen", "Hessen", "Mecklenburg-Vorpommern", "Niedersachsen", "Nordrhein-Westfalen"
                 "Rheinland-Pfalz", "Saarland", "Sachsen", "Sachsen-Anhalt", "Thüringen", "Berlin", "Schleswig-Holstein", "Hamburg"]
    mobi_data = mobi_data[mobi_data['State'].isin(de_states)]
    
    # ----------- tansform mobi data --------------
    # select specific column and make proper data for kba
    return mobi_data, kba_data


# ----------- L --------------
# load the data into 
def load_into_sqlite(mobi_data, kba_data, db_name):
    # import and create connection
    from sqlalchemy import create_engine
    engine = create_engine(f"sqlite:///../{db_name}.sqlite", echo=False)
    
    # load mobi data into db
    mobi_data.to_sql("mobi", con=engine, if_exists="replace", index=False)
    
    # load kba data into db
    kba_data.to_sql("kba", con=engine, if_exists="replace", index=False)
    
    return engine
    

def process_ETL():
    mobi_data = extract_mobi_data()
    kba_data = extract_kba_data()
    
    mobi_data, kba_data = transform_to_table(mobi_data, kba_data)
    
    engine = load_into_sqlite(mobi_data, kba_data, "data")

if __name__ == "__main__":
    process_ETL()
