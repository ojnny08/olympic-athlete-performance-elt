import pandas as pd
import numpy as np

def age_group(df_merge):
   df_merge['Age'] = df_merge['Games_Year'] - df_merge['Born_year']
   df_merge['Age'] = pd.to_numeric(df_merge['Age'], errors='coerce')
   # Group them accordingly
   age_bin = [13, 20, 30, 40, 50, 60, 70, 80]

   age_groups = np.array(['11-12', '13-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79'])

   index = np.digitize(df_merge['Age'].fillna(-1), age_bin)

   df_merge['Age_group'] = age_groups[index]

   # For the NA ages change the gorup to unknown
   df_merge.loc[df_merge['Age'].isna(), 'Age_group'] = 'Unknown'
   return df_merge

def podium_appearance_age(df_merge):
   df_podium = df_merge.groupby(['Games_Year', 'Age_group']).agg({
       'athlete_id' : 'count',
       'Medal' : 'count'
   }).reset_index()

   df_podium['Appearance_%'] = ((df_podium['Medal'] / df_podium['athlete_id']) * 100).round(2)

   df_podium = df_podium.rename(columns={
       'Games_Year': 'Year',
       'athlete_id': 'Total_Athletes',
       'Medal': 'Podium_Appearance',
   })
   print('Podium appearance age updated')
   return df_podium

def physical_preformance(df_merge):
   df_physcial = df_merge.groupby(['Games_Year', 'Discipline_clean', 'Preformance_Result']).agg({
       'height_cm': ['mean', 'std'],
       'weight_kg': ['mean', 'std']
   }).reset_index()

   print('Physical preformance updated')
   return df_physcial

def year_total_points(df_merge):
   df_total_points = df_merge.groupby(['Games_Year', 'Age_group'])['Points'].sum().reset_index()
   print('Total year points updated')
   return df_total_points