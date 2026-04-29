import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib
from pathlib import Path
import pandas as pd
import awswrangler as wr


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


def load_data(engine):
    athlena_db = os.getenv('ATHENA_DATABASE')
    s3_output = os.getenv('ATHENA_S3_OUTPUT')

    print("reading tables")
    df_athletes = wr.athena.read_sql_query(
        sql='SELECT * FROM athletes_v2',
        database=athlena_db,
        s3_output=s3_output,
        ctas_approach=False
    )
    
    df_results = wr.athena.read_sql_query(
        sql='SELECT * FROM results_v2;',
        database=athlena_db,
        s3_output=s3_output,
        ctas_approach=False
    )

    df_athletes = clean_athletes(df_athletes)
    df_results = clean_results(df_results)

    clean_table = {
        'athletes_clean': df_athletes,
        'results_clean': df_results
    }
    table_loop_clean(clean_table, engine)

    df_merge = pd.merge(df_results, df_athletes, on='athlete_id')

    return df_merge

def table_loop_clean(dic, engine):
    clean_bucket = os.getenv('S3_BUCKET_CLEAN')
    folder = 'clean'
    for table_name, df in dic.items():
        #load_to_sql(df, table_name, engine)
        load_to_s3(df, table_name, clean_bucket, folder)

def table_loop_transformed(dic, engine):
    clean_bucket = os.getenv('S3_BUCKET_CLEAN')
    folder = 'transformed'
    for table_name, df in dic.items():
        #load_to_sql(df, table_name, engine)
        load_to_s3(df, table_name, clean_bucket, folder)

def load_to_sql(df, table_name, engine):
   print(f"Loading {table_name}")
   try:
       df.to_sql(table_name, con=engine, if_exists='replace', index=False)
       print(f"Table {table_name} loaded to sql server")
   except Exception as e:
       print(f"Error loading to sql {e}")

def load_to_s3(df, table_name, bucket, folder):
    print(f"Loading {table_name}")
    try:
        path = f"s3://{bucket}/{folder}/{table_name}"
        wr.s3.to_parquet(df=df, path=path)
    except Exception as e:
        print(f"Error loading to s3 {e}")


if __name__ == "__main__":
    
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(dotenv_path=env_path)

    db_engine = engine()
    
    print("beginning")
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

    table_loop_transformed(table_save, db_engine)
        

    print('Pipeline execution complete')