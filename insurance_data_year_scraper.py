import requests
from bs4 import BeautifulSoup

# URL of the website
url = 'https://www.iii.org/table-archive/21407'

# Send a GET request to the URL and parse
# the HTML content using BeautifulSoup
html_text = requests.get(url).text
soup = BeautifulSoup(html_text, 'html.parser')


"""
The 'year', for each table, is contained within headings starting with 'text_to_find'.
As each heading is prepended with the 'span' HTML tag, we then check if the text
of each 'span' element contains 'text_to_find'.
"""

text_to_find = 'average premiums for homeowners and renters insurance'
filtered_elements = [element for element in soup.find_all('span') if (text_to_find in element.get_text().lower())]

year_list = []

for element in filtered_elements:
    
    text = element.get_text()

    # split 'text' by comma and then by space, retrieving the first value ('year')
    year = text.split(', ')[-1].split()[0]  
    year_list.append(year)

# print(year_list)