import pandas as pd

def clean_athletes(df_athletes):
    # Apply notebook cleaning
    df_athletes['Name'] = df_athletes['Used name'].str.replace('•', ' ')
    df_athletes['Name'] = df_athletes['Name'].str.replace('-', ' ')

    df_athletes['height_cm'] = df_athletes['Measurements'].str.extract(r'(\d+)\s*cm')
    df_athletes['weight_kg'] = df_athletes['Measurements'].str.extract(r'(\d+)\s*kg')

    date_pattern = r'(\d+ \w+ \d{4})'
    df_athletes['born_date'] = df_athletes['Born'].str.extract(date_pattern)
    df_athletes['death_date'] = df_athletes['Died'].str.extract(date_pattern)

    df_athletes['Born_year'] = df_athletes['Born'].str.extract(r'(\d{4})')
    df_athletes['Death_year'] = df_athletes['Died'].str.extract(r'(\d{4})')

    location_pattern = r'in (.*)'
    df_athletes['Birth_location'] = df_athletes['Born'].str.extract(location_pattern)

    seperation_pattern = r'([\w\s]+), ([\w\s]+) \((\w+)\)'
    df_athletes[['City', 'Region', 'Country']] = df_athletes['Birth_location'].str.extract(seperation_pattern)

    athlete_cols_int = ['athlete_id', 'Born_year', 'Death_year', 'height_cm', 'weight_kg']
    df_athletes[athlete_cols_int] = df_athletes[athlete_cols_int].apply(pd.to_numeric, errors='coerce').astype('Int64')

    athlete_cols_str = ['Name', 'Birth_location', 'City', 'Region', 'Country', 'NOC']
    df_athletes[athlete_cols_str] = df_athletes[athlete_cols_str].astype("string")

    columns_keep = ['athlete_id', 'Name', 'Sex', 'NOC', 'height_cm', 'weight_kg', 'born_date', 'death_date', 'Born_year', 'Death_year', 'City', 'Region', 'Country']
    df_athletes_clean = df_athletes[columns_keep]

    return df_athletes_clean

def clean_results(df_results):

    df_results['Pos_clean'] = df_results['Pos'].str.replace('=', ' ')

    # Use the start year of the event
    year = r'(\d{4})'
    df_results['Games_Year'] = df_results['Games'].str.extract(year)
    # Extract the season then set NA to the games without a season
    season_pattern = r'\d{4} (\w+)'
    df_results['Season'] = df_results['Games'].str.extract(season_pattern)
    season_list = ['Winter', 'Summer', 'Fall', 'Spring']
    df_results.loc[~df_results['Season'].isin(season_list), 'Season'] = pd.NA

    # Filter out males and females
    genders = r'\b(Men|Women)\b'
    df_results['Gender'] = df_results['Event'].str.extract(genders)

    discipline_pattern = r'\s\(.*\)'
    df_results['Discipline_clean'] = df_results['Discipline'].str.replace(discipline_pattern, ' ', regex=True)

    df_results['Name'] = df_results['As'].str.replace('-', ' ')

    event_pattern = r'(.*), '
    df_results['Event_clean'] = df_results['Event'].str.extract(event_pattern)

    # remove whitespace and change into lowercase to normalize
    df_results['Medal'] = df_results['Medal'].str.strip().str.lower()

    medal_map = {
        'gold': 3,
        'silver': 2,
        'bronze': 1,
    }

    df_results['Points'] = df_results['Medal'].map(medal_map).fillna(0)

    df_results['Preformance_Result'] = df_results['Points'].apply(lambda x: 'Medalist' if x > 0 else 'non-Medalist')

    results_columns_keep = ['athlete_id', 'Name', 'Gender', 'Discipline_clean', 'Event_clean', 'Medal', 'Points', 'Preformance_Result', 'Pos_clean', 'Games_Year', 'Season']
    df_results = df_results[results_columns_keep]

    # Convert datatype
    results_cols_str = ['Name', 'Gender', 'Discipline_clean', 'Event_clean', 'Medal', 'Season', 'Preformance_Result']
    results_cols_int = ['athlete_id', 'Points', 'Pos_clean', 'Games_Year']

    df_results_clean = df_results[results_cols_str + results_cols_int].copy()

    df_results_clean[results_cols_str] = df_results_clean[results_cols_str].astype('string')
    df_results_clean[results_cols_int] = df_results_clean[results_cols_int].apply(pd.to_numeric, errors='coerce').astype('Int64')
    
    return df_results_clean