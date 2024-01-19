from credentials import *

import pandas as pd
import numpy as np

# Modules for NOAA class
from google.cloud import bigquery
import concurrent.futures


# Modules for HomeRentalInsurance class
import requests
from bs4 import BeautifulSoup



class NOAA:

    def __init__(self, project_id, noaa_file_path):

        self.client = bigquery.Client(project = project_id)
        self.noaa_file_path = noaa_file_path
        self.years = range(1950, 2024)


    def export_from_bigquery(self, table_id):

        query = f"SELECT * FROM `bigquery-public-data.noaa_historic_severe_storms.{table_id}`"

        # Execute the query
        query_job = self.client.query(query)

        # Get the query results into a dataframe
        df = query_job.to_dataframe()

        return df


    def save_to_csv(self, df, table_id):

        output_file_path = f"{self.noaa_file_path}/{table_id}.csv"

        # Save the dataframe as a CSV file
        df.to_csv(output_file_path, index=False)

        print(f"CSV file saved to: {output_file_path}")


    def export_and_save(self, year):

        table_id = f"storms_{year}"
        df = self.export_from_bigquery(table_id)
        self.save_to_csv(df, table_id)


    def concurrent_export_and_save(self):

        # Enable multithreading
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.export_and_save, self.years)

        print(f"\nRetrieval of NOAA Historic Severe Storms data from {self.years[0]} to {self.years[-1]} complete.")

noaa_instance = NOAA(project_id, noaa_file_path)

print(f"\nInitiating retrieval and storage of data from the NOAA Historic Severe Storms dataset.\n")
noaa_instance.concurrent_export_and_save()






class HomeRentalInsurance:

    def __init__(self, url):

        self.url = url
        self.html_text = requests.get(url).text
        self.soup = BeautifulSoup(self.html_text, "html.parser")

        self.text_to_find = "average premiums for homeowners and renters insurance"
        self.filtered_elements = [element for element in self.soup.find_all("span") if (self.text_to_find in element.get_text().lower())]

        self.year_list = []
        self.new_list = []
    
    def extract_years(self):

        for element in self.filtered_elements:
            text = element.get_text()

            year = text.split(", ")[-1].split()[0]
            self.year_list.append(year)

        # print(self.year_list)


    def process_tables(self):

        num_tables = len(self.soup.find_all('table'))
        year_i = 0

        for i in range(1, num_tables, 2):

            table = self.soup.find_all('table')[i]
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

                row_data.insert(0, self.year_list[year_i])
                row_data.insert(6, self.year_list[year_i])

                # Split each list into 2 sublists, and append them separately
                self.new_list.append(row_data[:6])
                self.new_list.append(row_data[6:])

            year_i += 1


    def create_dataframe(self):

        # Specify the dataframe columns, and convert the 'new_list' to a dataframe
        # NOTE: that the 'rank' columns are by year.        
        df_columns = ['year', 'state', 'homeowners_avg_premium', 'homeowners_rank', 'renters_avg_premium', 'renters_rank']
        df = pd.DataFrame(self.new_list, columns = df_columns)

        # Remove the '$' character from each of the 3 columns listed below:
        for column in ['homeowners_avg_premium', 'homeowners_rank', 'renters_avg_premium']:
            df[column] = df[column].str.replace('$', '')

        # Remove the ',' character from the 1 column:
        df['homeowners_avg_premium'] = df['homeowners_avg_premium'].str.replace(',', '')

        # Replace None with NaN and drop rows with NaN values
        df = df.fillna(value = np.nan)
        df_cleaned = df.dropna()

        df_cleaned = df_cleaned.astype({"year": int,
                                        "state": str,
                                        "homeowners_avg_premium": float,
                                        "homeowners_rank": int,
                                        "renters_avg_premium": float,
                                        "renters_rank": int
                                        })

        df_cleaned.sort_values(by = ["year", "state"], inplace = True)
        df_cleaned.reset_index(drop = True, inplace = True)

        return df_cleaned


    def display_and_save_data(self, df_cleaned, output_file_path):

        # Change the settings to display all rows
        pd.set_option("display.max_rows", None)

        print(f"\nInitiating retrieval and storage of Homeowners and Renters Insurance by State dataset.\n")
        print(f"Displaying the dataframe...")
        print(df_cleaned)

        df_cleaned.to_csv(output_file_path, index = False)

        print(f"\nRetrieval of Homeowners and Renters Insurance by State dataset complete.")
        print(f"The dataset can be found within the following repository: {output_file_path}.")


url = "https://www.iii.org/table-archive/21407"

insurance_instance = HomeRentalInsurance(url)
insurance_instance.extract_years()
insurance_instance.process_tables()
df_cleaned = insurance_instance.create_dataframe()

output_file_path = f"{insurance_file_path}/insurance_by_year_and_state.csv"
insurance_instance.display_and_save_data(df_cleaned, output_file_path)