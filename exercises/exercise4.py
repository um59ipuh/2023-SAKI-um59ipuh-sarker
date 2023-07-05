import pandas as pd
import logging as log
import urllib.request
import zipfile
import os

# ------------ Extract data from URL (E) -----------
def extract_data(url):
    # extract zip file and save as csvfile
    zip_file_path = 'mowesta-dataset.zip'
    
    urllib.request.urlretrieve(url, zip_file_path)

    # Extract the CSV file from the ZIP
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        # Assuming the CSV file is named 'data.csv'
        csv_file_name = 'data.csv'
        extracted_csv_path = zip_ref.extract(csv_file_name)

    intended_types = {
        'Temperatur in °C (DWD)': 'float',
        'Batterietemperatur in °C': 'float'
    }

    # Read the extracted CSV file into a Pandas DataFrame
    df = pd.read_csv(extracted_csv_path, delimiter=";", decimal=',', usecols=range(11), dtype=intended_types, header=None)
    df.columns = df.iloc[0]
    df.drop(index=0, inplace=True)
    log.info(f"Extract data from : {url}.")

    # clean data
    os.remove(zip_file_path)
    os.remove(csv_file_name)
    
    return df

def reshape_date(df):
    # select specific column
    selected_columns = [
        'Geraet',
        'Hersteller',
        'Model',
        'Monat',
        'Temperatur in °C (DWD)',
        'Batterietemperatur in °C',
        'Geraet aktiv'
    ]
    df = df[selected_columns]
    
    # Rename selected columns
    column_mapping = {
        'Temperatur in °C (DWD)': 'Temperatur',
        'Batterietemperatur in °C': 'Batterietemperatur'
    }
    df = df.rename(columns=column_mapping)
    
    # drop duplicate columns
    # df = df.loc[:,~df.columns.duplicated()].copy()
    
    return df

# ------------ Transform data (T) -----------
def transform(df):
    # transform column value from celsius to farenheit
    df['Temperatur'] = (df['Temperatur'].apply(lambda x: x.replace(',','.')).astype(float) * 9/5) + 32
    df['Batterietemperatur'] = (df['Batterietemperatur'].apply(lambda x: x.replace(',','.')).astype(float) * 9/5) + 32
    
    return df

def validate_date(df):
    # take only row with values more than 0
    df = df[df['Geraet'].apply(int) > 0]
    
    return df


def load_into_db(data, db_name, table_name, sqlite_types):
    # import module and create connection
    # from sqlalchemy import create_engine
    import sqlite3
    
    # engine = create_engine(f"sqlite:///{db_name}.sqlite", echo=False)
    with sqlite3.connect(db_name) as conn:
        data.to_sql(table_name, conn, index=False, if_exists='replace', dtype=sqlite_types)

def processETL():
    # extract the data
    url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
    db_name = 'temperatures.sqlite'
    table_name = 'temperatures'
    
    # Define SQLite types for each column
    sqlite_types = {
        'Geraet': 'BIGINT',
        'Hersteller': 'TEXT',
        'Model': 'TEXT',
        'Monat': 'BIGINT',
        'Temperatur': 'FLOAT',
        'Batterietemperatur': 'FLOAT',
        'Geraet aktiv': 'TEXT'
    }
    
    # ETL Pipeline
    df = extract_data(url)
    shaped_data = reshape_date(df)
    transformed_data = transform(shaped_data)
    validated_data = validate_date(transformed_data)
    
    # load into db
    engine = load_into_db(validated_data, db_name, table_name, sqlite_types)
    
if __name__ == '__main__':
    processETL()