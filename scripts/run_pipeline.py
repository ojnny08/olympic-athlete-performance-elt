from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import urllib

def load_data():
    athletes = pd.read_csv('../data/processed/athletes.csv')
    results = pd.read_csv('../data/processed/results.csv')
    return results.merge(athletes, on='athlete_id')

def age_group(df_master):
    df_master['Age'] = df_master['Games_Year'] - df_master['Born_year']
    df_master['Age'] = pd.to_numeric(df_master['Age'], errors='coerce')

    # Group them accordingly
    age_bin = [13, 20, 30, 40, 50, 60, 70, 80]

    age_groups = np.array(['11-12', '13-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79'])

    index = np.digitize(df_master['Age'].fillna(-1), age_bin)

    df_master['Age_group'] = age_groups[index]

    # For the NA ages change the gorup to unknown
    df_master.loc[df_master['Age'].isna(), 'Age_group'] = 'Unknown'
    return df_master

def podium_appearance_age(df_master):
    df_podium = df_master.groupby(['Games_Year', 'Age_group']).agg({
        'athlete_id' : 'count',
        'Medal' : 'count'
    }).reset_index()

    df_podium['Appearance_%'] = ((df_podium['Medal'] / df_podium['athlete_id']) * 100).round(2)

    df_podium = df_podium.rename(columns={
        'Games_Year': 'Year',
        'athlete_id': 'Total_Athletes',
        'Medal': 'Podium_Appearance',
    })

    df_podium.to_csv('./clean-data/podium_appearances_age.csv', index=False)
    print('Podium appearance age updated')

def physical_preformance(df_master):
    df_physcial = df_master.groupby(['Games_Year', 'Discipline_clean', 'Preformance_Result']).agg({
        'height_cm': ['mean', 'std'],
        'weight_kg': ['mean', 'std']
    }).reset_index()

    df_physcial.to_csv('./clean-data/physical_preformance.csv', index=False)
    print('Physical preformance updated')

def year_total_points(df_master):
    df_total_points = df_master.groupby(['Games_Year', 'Age_group'])['Points'].sum().reset_index()

    df_total_points.to_csv('./clean-data/year_total_points.csv', index=False)
    
def load_to_sql(df, table_name, engine):

    print(f"Loading {table_name}...")
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Table {table_name} successfully loaded to SQL Server")
    except Exception as e:
        print(f"Error loading to SQL: {e}")

def engine():
    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=Olympic_Data;"
        "UID=sa;"
        "PWD=Admin@123;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    params = urllib.parse.quote_plus(connection_string)

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

if __name__ == "__main__":
    master = load_data()
    age_group(master)
    podium_appearance_age(master)
    physical_preformance(master)
    year_total_points(master)

    db_engine = engine()

    files_to_load = {
        'athletes.csv': 'athletes',
        'results.csv': 'results',
        'master.csv': 'master',
        'physical_preformance.csv': 'physical_preformance',
        'podium_appearances_age.csv': 'podium_appearances_age',
        'year_total_points.csv': 'year_total_points',
    }

    for file, table_name in files_to_load.items():
        try:
            BASE_DIR = Path(__file__).resolve().parent.parent/ "data" / "processed"

            df = pd.read_csv(BASE_DIR / file)
            load_to_sql(df, table_name, db_engine)

        except FileNotFoundError:
            print(f"{table_name} not found")
        except Exception as e:
            print(f"Failed to load {table_name}: {e}")

    print('Pipeline execution complete')
