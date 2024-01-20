# Import packages
import requests
from credentials import insurance_file_path
from bs4 import BeautifulSoup

import pandas as pd
import numpy as np
from insurance_data_year_scraper import year_list

url = 'https://www.iii.org/table-archive/21407'

# Send a GET request to the URL and parse
# the HTML content using BeautifulSoup
html_text = requests.get(url).text
soup = BeautifulSoup(html_text, 'html.parser')


num_tables = len(soup.find_all('table'))

new_list = []
year_i = 0


for i in range(1, num_tables, 2):
    """
    Iterate through each table from the 'soup' object
    and find all rows within the table (as denoted by
    the 'tr' flag)
    """
    
    table = soup.find_all('table')[i]
    rows = table.find_all('tr')

    data = []

    for row in rows:

        """
        For each row, find all of the data ('td') and header ('th')
        tags. Then, convert the HTML strings into plain text
        using .text.strip() to remove non breaking spaces
        (all instances of '\xa0')
        """
        cols = row.find_all(['td', 'th'])
        cols = [ele.text.strip() for ele in cols]

        # Append non-empty elements to 'data'
        data.append([ele for ele in cols if ele])

    
    for row_data in data[2:]:

        """
        For each row in 'data', starting from the 2nd index
        to exclude table headers, insert the corresponding
        year (w/ reference to insurance_data_year_scraper.py) to each row.

        Since each row contains 2 records / states, the year
        is inserted before each 'state' at the 0 and 6th index.
        """

        row_data.insert(0, year_list[year_i])
        row_data.insert(6, year_list[year_i])
        
        # Split each list into 2 sublists, and append them separately
        new_list.append(row_data[:6])
        new_list.append(row_data[6:])

    year_i += 1

# print(new_list)

# Specify the dataframe columns, and convert the 'new_list' to a dataframe
df_columns = ['year', 'state', 'homeowners_avg_premium', 'homeowners_rank', 'renters_avg_premium', 'renters_rank']
df = pd.DataFrame(new_list, columns = df_columns)

# Remove the '$' character from each of the 3 columns listed below:
for column in ['homeowners_avg_premium', 'homeowners_rank', 'renters_avg_premium']:
    df[column] = df[column].str.replace('$', '')

# Remove the ',' character from the 1 column:
df['homeowners_avg_premium'] = df['homeowners_avg_premium'].str.replace(',', '')


# Replace None with NaN and drop rows with NaN values
df = df.fillna(value = np.nan)
df_cleaned = df.dropna()


df_cleaned = df_cleaned.astype({'year': int, 
                                'state': str, 
                                'homeowners_avg_premium': float, 
                                'homeowners_rank': int, 
                                'renters_avg_premium': float, 
                                'renters_rank': int})


df_cleaned.sort_values(by = ['year', 'state'], inplace = True)
df_cleaned.reset_index(drop = True, inplace = True)

# Change the settings to display all rows
pd.set_option('display.max_rows', None)
print(df_cleaned)

output_file_path = f"{insurance_file_path}/insurance_by_year_and_state.csv"
df_cleaned.to_csv(output_file_path, index = False)