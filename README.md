## Data Engineering: Capstone Project

### Introduction
Given the continuously evolving nature of our climate, this project seeks to analyze the evolution of severe weather events in the United States (on a year-over-year basis), across various event types. By leveraging the _NOAA: Severe Storm Event Details_ dataset (linked below), alongside additional secondary sources, this project seeks to uncover the effects and implications of severe weather events on home insurance premiums and interstate migration flows.

As such, this project will seek to address the following:

1. **Severe Weather Event Occurrences:**</br>
    Is there an observable trend in the occurrence of severe weather events?</br>
    Is it possible to observe an evolution, in which certain areas or states have recently become more prone to such events?</br>
    Of the various event types, are certain weather events occurring with greater frequency?

2. **Migration Patterns:**</br>
    Are individuals and/or families migrating to states where the occurrence of severe weather events is greatest?</br>
    Are individuals and/or families migrating to states where home insurance premiums are rising the fastest, due to the greater risk of severe weather events occurring?

3. **Home Insurance Premiums and Risk Assessment:**</br> 
    Is there a correlation between the rise in home insurance premiums and the heightened risk associated with severe weather events in certain states?


### Data Collection
---
In support of this project, the data sources listed below will be leveraged in order to collect and compile the necessary data to achieve the above. The data collection process is comprised of the following:

1. `noaa_historic_severe_storms_gbq_data`</br>
    Used to export and save each of the `storm_` tables from BigQuery as a local CSV file (from 1950 onward).

2. `insurance_data_scraper`</br>
    Retrieves data from each of the tables hosted by the designated URL (_see Data Source #2 below_) and compiles the data into a single unified dataframe representing the Average Homeowners and Renters Insurance Premium by Year and State (from 2007 through 2020). 

3. `census_state_migration_xls_scraper`</br>
    Used to export and locally save each of the Excel files hosted by the designated URL (_see Data Source #3 below_). In support of this step, a separate script (`census_state_migration_cleaned_xls`) is leveraged in order to reconfigure and reformat the information for analysis.


</br>

### Data Sources
---
1. NOAA: Severe Storm Event Details [(Link)](https://console.cloud.google.com/marketplace/product/noaa-public/severe-storm-events)

2. Homeowners and Renters Insurance by State [(Link)](https://www.iii.org/table-archive/21407)

3. State-to-State Migration Flows [(Link)](https://www.census.gov/data/tables/time-series/demo/geographic-mobility/state-to-state-migration.html)