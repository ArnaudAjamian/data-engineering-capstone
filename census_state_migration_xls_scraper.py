# Import packages
import requests
import urllib.parse
import os
from bs4 import BeautifulSoup
from credentials import census_raw_files_folder


# Create a new folder containing all of the Excel files from 'url'
output_folder = "State to State Migration Flows - Raw Excel Data"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)


url = 'https://www.census.gov/data/tables/time-series/demo/geographic-mobility/state-to-state-migration.html'

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
    file_name = os.path.join(census_raw_files_folder, os.path.basename(file_url).lower())

    print(f"\nDownloading {file_url}...")

    # Send a GET request to download the file
    file_response = requests.get(file_url)

    # Save the file to 'census_raw_files_folder'
    with open(file_name, 'wb') as file:
        file.write(file_response.content)

    print(f"File saved as {file_name}")