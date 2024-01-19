from credentials import *

import pandas as pd
import numpy as np

# Modules for NOAA class
from google.cloud import bigquery
import concurrent.futures


# Modules for HomeRentalInsurance and CensusMigration class
import requests
from bs4 import BeautifulSoup


# Modules for CensusMigration class
import urllib.parse
import os


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






class CensusMigration:

    def __init__(self):
        self.census_raw_files_folder = "State to State Migration Flows - Raw Excel Data"
        self.census_cleaned_files_folder = "State to State Migration Flows - Cleaned Excel Data"

    def download_raw_data(self, url):
        
        # Create a new folder containing all of the Excel files from 'url'
        if not os.path.exists(self.census_raw_files_folder):
            os.makedirs(self.census_raw_files_folder)

        # Send a GET request to the URL and parse
        # the HTML content using BeautifulSoup
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, 'html.parser')

        # Find all 'a' tags containing links to Excel files
        excel_links = soup.select('a[href$=".xls"]')

        for link in excel_links:

            """
            For each link for which an Excel file has been found (ending in .xls),
            generate a complete URL by joining the base URL with the one found
            following the 'href' attribute from the 'a' tag (represented as 'file_url'). 
            """

            file_url = urllib.parse.urljoin(url, link['href'])
            file_name = os.path.join(self.census_raw_files_folder, os.path.basename(file_url).lower())

            print(f"\nDownloading {file_url}...")

            # Send a GET request to download the file
            file_response = requests.get(file_url)

            # Save the file to 'census_raw_files_folder'
            with open(file_name, 'wb') as file:
                file.write(file_response.content)

            print(f"File saved as {file_name}")


    def process_excel_file(self, file_path):
        
        # Read in the Excel file and convert it to a dataframe.
        # Set the column headers equal to rows 6 and 7.
        df = pd.read_excel(file_path, header=[6, 7])


        """
        Over time, the format of the 'State to State Migration' tables has changed.
        From 2010 onward, additional columns were prepended to the original tables
            which shifted the target data:
            
            ie. 'Alabama' was presented in column B from 2005 - 2009
                'Alabama' is presented in column J from 2010 onward

        Since the target data, interstate migration figures, is tabulated from the
            'Alabama' column onward, we must first capture its column index.  
        """
        alabama_index = np.where(df.columns.get_loc('Alabama'))[0][0]

        # Slice the dataframe to exclude those columns which do not represent
        # state to state migration figures (applicable for all tables from 2010 onward)
        df = df.iloc[:, [0] + list(range(alabama_index, df.shape[1]))]

        # Delete any blank rows from the dataframe since the states listed in column A
        # are generally partitioned in groups of 5.
        df = df.dropna(axis=0, how='all')

        """
        In the original Excel file, there are multi-index column headers.
        - Level 1 = State Name (row 7)
        - Level 2 = Estimate and MOE (Margin of Error) (row 8)

        Below, the 1st level (State Name) is converted into a separate column.
        """
        df = df.set_index(df.columns[0]) \
                .stack(level=0) \
                .reset_index() \
                .rename({df.columns[0]: 'Moved To: State',
                        'level_1': 'Moved From: State'}, axis=1)

        # Filter out any rows where the 'Moved To' and 'Moved From' states are equal
        # since we only want to capture interstate migration flows.
        df = df[(df['Moved To: State'] != df['Moved From: State'])
                & (~df['Moved To: State'].str.contains("United States", na=False))]

        df.drop(['MOE.1', 'Estimate.1'], axis=1, inplace=True, errors='ignore')
        df = df.dropna(axis=0, how='any')

        df.reset_index(drop=True, inplace=True)

        return df



    def process_raw_data(self):

        # Define the folder containing the Excel files
        # and get a list of Excel files in the folder
        folder_path = f"{self.census_raw_files_folder}"
        file_list = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.xls')]

        if not os.path.exists(self.census_cleaned_files_folder):
            os.makedirs(self.census_cleaned_files_folder)

        for file in file_list:
            if not file.endswith("state_migration_flows_tables.xls"):

                result_df = self.process_excel_file(file)

                # Retrieve the original file name without extension and replace "table" with "dataframe"
                original_file_name = os.path.splitext(os.path.basename(file))[0]
                new_file_name = original_file_name.replace("table", "dataframe") + ".xlsx"

                # Define the path which will store the results of the 'process_excel_file' function
                processed_file_path = os.path.join(self.census_cleaned_files_folder, new_file_name)

                # Save the processed DataFrame to the designated folder
                result_df.to_excel(processed_file_path, index=False)
                print(f"\nSaving the processed raw data files...")
                print(f"The '{new_file_name}' file has been saved to the '{self.census_cleaned_files_folder}' folder.")



census_migration = CensusMigration()
census_migration.download_raw_data('https://www.census.gov/data/tables/time-series/demo/geographic-mobility/state-to-state-migration.html')
census_migration.process_raw_data()