import pandas as pd
import transformer as tr
import sqlite3

def load_data_into_sql():
    # Establish a connection to the SQLite database
    conn = sqlite3.connect('data.sqlite')
    
    # load mobilithek data
    df_mobi = tr.transform_mobilithek()
    df_kba = tr.transform_kba()
    
    df_mobi.to_sql('stations', conn, if_exists='replace', index=False)
    df_kba.to_sql('sales', conn, if_exists='replace', index=False)
    
    
    conn.close()
    

if __name__ == '__main':
    load_data_into_sql()