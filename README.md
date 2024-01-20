## Data Engineering: Capstone Project

### Introduction
Given the continuously evolving nature of our climate, this project seeks to analyze the evolution of severe weather events in the United States (on a year-over-year basis), across various event types. By leveraging the _NOAA: Severe Storm Event Details_ dataset (_see Data Source #1 below_), alongside additional sources, this project seeks to uncover the effects and implications of severe weather events on home insurance premiums and interstate migration flows.

As such, this project will seek to address the following:

1. **Severe Weather Event Occurrences:**</br>

    - Is there an observable trend in the occurrence of severe weather events?</br>
    - Is it possible to observe an evolution, in which certain areas or states have recently become more prone to such events?</br>
    - Of the various event types, are certain weather events occurring with greater frequency?

2. **Migration Patterns:**</br>

    - Are individuals and/or families migrating to states where the occurrence of severe weather events is rising?</br>
    - Are individuals and/or families migrating to states where home insurance premiums are rising due to the greater risk of severe weather events occurring?

3. **Home Insurance Premiums and Risk Assessment:**</br> 

    - Is there a correlation between the rise in home insurance premiums and the heightened risk associated with severe weather events in certain states?


### Data Collection
---
The data sources listed below will be leveraged in order to collect and compile the necessary data to achieve the above. The data collection process is comprised of the following:
_(Refer to the `Python Scripts` folder)_

#### Phase 1 - Create separate data collection scripts:

1. `noaa_historic_severe_storms_gbq_data.py`</br>
    Used to export and save each of the `storm_` tables from BigQuery as a local CSV file (from 1950 - 2023).

2. `insurance_data_scraper.py`</br>
    Retrieves data from each of the tables hosted by the designated URL (_see Data Source #2 below_) and compiles the data into a single unified dataframe representing the 'Average Homeowners and Renters Insurance Premium by Year and State' (from 2007 through 2020). In order to associate each table with its respective year, a separate script (`insurance_data_year_scraper.py`) facilitates the retrieval of this information from each table's heading. 

3. `census_state_migration_xls_scraper.py`</br>
    Used to export and locally save each of the Excel files hosted by the designated URL (_see Data Source #3 below_). In support of this step, a separate script (`census_state_migration_cleaned_xls.py`) is leveraged to reconfigure and reformat the information for analysis.

#### Phase 2 - Integrate each of the data collection scripts (from Phase 1) into a single 'Master OOP Script':

Contained within the `master_data_pipeline_oop_script.py` file (within the Python Scripts folder) are 3 separate classes (namely: **NOAA**, **HomeRentalInsurance** and **CensusMigration**) which make up the data pipeline. This script seeks to automate the retrieval, cleaning / transformation and storage of data from each of the 3 data sources listed below.

Whereas logging had not been implemented during Phase 1, it has been incorporated into the `master_data_pipeline_oop_script.py` file in order to log information, to both the terminal and to a separate 'Logs' directory, at each step of the data retrieval process.

</br>

### Exploratory Data Analysis
##### (_Based on the most recent year for which data is available for each of the respective datasets_)
---
Having acquired the data for this capstone project from BigQuery and various webpages using web scraping techniques, we may now turn our attention to pre-processing the data for Exploratory Data Analysis (refer to the Jupyter Notebook: `Exploratory Data Analysis.ipynb`). In conducting Exploratory Data Analysis, the most recent data from each dataset will be imported and loaded into a dataframe. Then, each of the datasets are to be pre-processed and cleaned (accounting for any missing values) so that the information can be analyzed to generate insights. 

Using Matplotlib, Seaborn and Plotly, the pre-processed data from each source is leveraged to explore the following themes:

1. **NOAA: Severe Storm Event Details (2022)**
- Top 5: Most Frequenty Severe Weather Event Types
- Number of Severe Weather Events by State

2. **Homeowners and Renters Insurance by State (2007 - 2020)**
- Average Homeowners' and Renters' Insurance Premiums by Year
- Average Homeowners' Insurance Premium by State

3. **State to State Migration Flows (2021)**
- Net Migration by State
- Interstate Migration to Florida and Texas
</br>

### Setting Up the Requisite Conda Environment
---

### Exploratory Data Analysis
To set-up the necessary conda environment, please refer to the `eda_requirements.txt` file. This will enable you to create the necessary environment to run the `Exploratory Data Analysis` Jupyter Notebook.

Using either the terminal or Anaconda Prompt:

1. Create the environment from the `eda_requirements.txt` file:
    ```python
    conda env -n <environment_name> --file eda_requirements.txt
    ```

2. Activate the new environment:</br>
    ```python
    conda activate <environment_name>
    ```

3. To add kernel to the active conda environment:
    ```
    python -m ipykernel install --user --name <environment_name> --display-name "<environment_name>"
    ```
</br>

### Data Sources
---
1. NOAA: Severe Storm Event Details [(Link)](https://console.cloud.google.com/marketplace/product/noaa-public/severe-storm-events)

2. Homeowners and Renters Insurance by State [(Link)](https://www.iii.org/table-archive/21407)

3. State-to-State Migration Flows [(Link)](https://www.census.gov/data/tables/time-series/demo/geographic-mobility/state-to-state-migration.html)