# TestETL.py
import pytest
import ETL as etl
import os.path as path
import pandas as pd


def test_extract_mobi_data():
    col_str = "betreiber;art_der_ladeeinrichung;anzahl_ladepunkte;anschlussleistung;steckertypen1;steckertypen2;steckertypen3;steckertypen4;p1_kw;p2_kw;p3_kw;p4_kw;kreis_kreisfreie_stadt;ort;postleitzahl;strasse;hausnummer;adresszusatz;inbetriebnahmedatum;koordinaten"
    col_list = col_str.split(';')
    df = etl.extract_mobi_data()
    # test if columns are equals
    assert col_list == list(df)
    # test if file exists
    file_path = "./mobi-data-raw.csv"
    assert path.isfile(file_path) == True
    
def test_extract_kba_data():
    col_list = ["State", "EV Sales"]
    df = etl.extract_kba_data()
    # test if columns are equals
    assert col_list == list(df)
    # test if file exists
    file_path = "./kba-data-raw.csv"
    assert path.isfile(file_path) == True
    # test if number of rows
    assert len(df.index) == 16

def test_transform_to_table():
    mobi_data = pd.read_csv("./mobi-data-raw.csv")
    kba_data = pd.read_csv("./kba-data-raw.csv")
    mobi, kba = etl.transform_to_table(mobi_data, kba_data)
    de_states = ["Baden-Württemberg", "Bayern", "Brandenburg", "Bremen", "Hessen", "Mecklenburg-Vorpommern", "Niedersachsen", "Nordrhein-Westfalen"
                 "Rheinland-Pfalz", "Saarland", "Sachsen", "Sachsen-Anhalt", "Thüringen", "Berlin", "Schleswig-Holstein", "Hamburg"]
    
    # test mobi data here
    test_flag = True
    for item in mobi['State'].to_list():
        if item not in de_states:
            test_flag = False
    
    assert test_flag == True
    
    # test transformed kba data here
    assert len(kba.index) == 16

def test_load_into_sqlite():
    database = "data"
    mobi_data = pd.read_csv("./mobi-data-raw.csv")
    kba_data = pd.read_csv("./kba-data-raw.csv")
    dataset1, dataset2 = etl.transform_to_table(mobi_data, kba_data)
    engine = etl.load_into_sqlite(dataset1, dataset2, database)
    
    # read data from databases and test
    mobidb = pd.read_sql_table("mobi", f"sqlite:///../{database}.sqlite")
    assert len(mobidb.index) == len(dataset1.index)
    
    # read data from databases and test
    kbadb = pd.read_sql_table("kba", f"sqlite:///../{database}.sqlite")
    assert len(kbadb.index) == len(dataset2.index)
    
def test_clear_all_environment():
    # delete two csv(*-raw.csv) files
     
    pass