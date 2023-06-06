import pandas as pd
import logging as log

# ------------ Extract data from URL (E) -----------
def extract_data(url):
    df = pd.read_csv(url, delimiter=';')
    log.info(f"Extract data from : {url}.")
    return df


def to_float(s: str) -> float:
    s = s.replace(',', '.')
    return float(s)

# ------------ Transform data (T) -----------
def transform(df):
    # drop Status column
    df.drop('Status', axis=1, inplace=True)
    log.debug(f"Drop column : Status.")
    # ----- drop all rows contains invalid value
    # - Empty cells are considered invalid
    df.dropna(subset=df.columns.to_list(), inplace=True)
    log.debug("Drop all rows contains NULL values.")
    # - Valid "Verkehr" values are "FV", "RV", "nur DPN"
    valid_set = ["FV", "RV", "nur DPN"]
    df = df[df['Verkehr'].isin(valid_set)]
    log.debug("Check Verkehr colunn value for validity.")
    # - Valid "Laenge", "Breite" values are geographic coordinate system values between -90 and 90
    df['Laenge'] = df['Laenge'].apply(to_float)
    df['Breite'] = df['Breite'].apply(to_float)
    df = df[df['Laenge'].between(-90., 90.) & df['Breite'].between(-90., 90.)]
    log.debug('Convert Laenge and Breite to float and check thier validity.')
    # - Valid "IFOPT" values follow this pattern: <exactly two characters>:<any amount of numbers>:<any amount of numbers><optionally another colon followed by any amount of numbers>
    pattern = r'\b\w{2}:\d+:\d+(:\d+)?\b'
    df = df[df['IFOPT'].str.contains(pattern)]
    log.debug('Check pattern and validate.')
    return df
    pass


def load_into_db(data, db_name, table_name):
    # import module and create connection
    from sqlalchemy import create_engine
    engine = create_engine(f"sqlite:///{db_name}.sqlite", echo=False)
    data.to_sql(table_name, con=engine, if_exists="replace", index=False)
    return engine

def processETL():
    # extract the data
    url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    df = extract_data(url)
    
    # transform the dataset
    table_data = transform(df)
    
    # load into db
    db_name = "trainstops"
    table_name = "trainstops"
    engine = load_into_db(table_data, db_name, table_name)
    
if __name__ == '__main__':
    processETL()