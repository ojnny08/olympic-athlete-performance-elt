import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib
from pathlib import Path
import pandas as pd
#from scripts.spark_builder import spark


from cleaning import clean_athletes, clean_results
from transformations import age_group, physical_preformance, podium_appearance_age, year_total_points

def engine():
    db_password = os.getenv("DB_PASSWORD")
    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=Olympic_Data;"
        "UID=sa;"
        f"PWD={db_password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    params = urllib.parse.quote_plus(connection_string)

    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

def initial_load_to_sql(engine):
    table = {
        'athletes': '../data/raw/athletes.csv',
        'results': '../data/raw/results.csv'
    }

    for table, file_path in table.items():
        print(f"Loading {table}...")
        try:
            df = pd.read_csv(file_path)
            df.to_sql(table, con=engine, if_exists='replace', index=False)
            print(f"Successfully loaded {table} to Docker.")
        except Exception as e:
            print(f"Failed loading {table} to Docker: {e}")

def read_query(file_path):
    with open(file_path, 'r') as file:
        query = file.read()
    return query

def load_data(engine):
    athletes_path = ('../queries/athletes_query.sql')
    results_path = ('../queries/results_query.sql')

    read_athletes = read_query(athletes_path)
    read_results = read_query(results_path)

    df_athletes = pd.read_sql(read_athletes, engine)
    df_results = pd.read_sql(read_results, engine)

    athletes = clean_athletes(df_athletes)
    results = clean_results(df_results)

    clean_table = {
        'athletes_clean': athletes,
        'results_clean': results
    }
    table_loop(clean_table, engine)
    columns = ['athlete_id','height_cm', 'weight_kg', 'Born_year', 'Death_year', 'Country']

    merge = results.merge(athletes[columns], on='athlete_id', how='left')
    return merge

def table_loop(dic, engine):
    for table_name, df in dic.items():
        load_to_sql(df, table_name, engine)
        load_to_hadoop(df, table_name)

def load_to_sql(df, table_name, engine):

   print(f"Loading {table_name}...")
   try:
       df.to_sql(table_name, con=engine, if_exists='replace', index=False)
       print(f"Table {table_name} successfully loaded to SQL Server")
   except Exception as e:
       print(f"Error loading to SQL: {e}")

def load_to_hadoop(df, file_name):
    try:
        df.write.mode('overwrite').parquet(f"hdfs:///data/clean/{file_name}")
    except Exception as e:
       print(f"Error loading to Hadoop: {e}")
    


if __name__ == "__main__":
    
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(dotenv_path=env_path)

    db_engine = engine()

    #initial_load_to_sql(db_engine)
    
    merge = load_data(db_engine)
    master = age_group(merge)
    podium_df = podium_appearance_age(master)
    physical_df = physical_preformance(master)
    points_df = year_total_points(master)

    table_save = {
        'Master_table': master,
        'podium_appearance_age': podium_df,
        'physical_preformance': physical_df,
        'year_total_points': points_df,
    }

    table_loop(table_save, db_engine)
        

    print('Pipeline execution complete')