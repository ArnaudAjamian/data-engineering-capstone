import os
import pandas as pd
import numpy as np
from credentials import census_raw_files_folder


# Create a new folder containing all of the reformatted Excel files
output_folder = "State to State Migration Flows - Cleaned Excel Data"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)


def process_excel_file(file_path):

    # Read in the Excel file and convert it to a dataframe.
    # Set the column headers equal to rows 6 and 7.
    df = pd.read_excel(file_path, header = [6, 7])


    """
    Over time, the format of the 'State to State Migration' tables has changed.
    From 2010 onward, additional columns were prepended to the original tables
        which shifted the target data:
         
        ie. 'Alabama' was presented in column B from 2005 - 2009
            'Alabama' is presented in column J from 2010 onward

    Since the target data, interstate migration figures, is tabulated from the
        'Alabama' column onward, we must first capture its column index.  
    """
    alabama_index = (np.where(df.columns.get_loc('Alabama'))[0][0])


    # Slice the dataframe to exclude those columns which do not represent
    # state to state migration figures (applicable for all tables from 2010 onward)
    df = df.iloc[:, [0] + list(range(alabama_index, df.shape[1]))]

    # Delete any blank rows from the dataframe since the states listed in column A
    # are generally partitioned in groups of 5.
    df = df.dropna(axis = 0, how = 'all')

    """
    In the original Excel file, there are multi-index column headers.
    - Level 1 = State Name (row 7)
    - Level 2 = Estimate and MOE (Margin of Error) (row 8)

    Below, the 1st level (State Name) is converted into a separate column.
    """
    df = df.set_index(df.columns[0]) \
            .stack(level = 0) \
            .reset_index() \
            .rename({df.columns[0]: 'Moved To: State',
                    'level_1': 'Moved From: State'}, axis = 1)

    # Filter out any rows where the 'Moved To' and 'Moved From' states are equal
    # since we only want to capture interstate migration flows.
    df = df[(df['Moved To: State'] != df['Moved From: State'])
            & (~df['Moved To: State'].str.contains("United States", na = False))]

    df.drop(['MOE.1', 'Estimate.1'], axis = 1, inplace = True, errors = 'ignore')
    df = df.dropna(axis = 0, how = 'any')

    df.reset_index(drop = True, inplace = True)
    
    return df




# The following section leverages the above 'process_excel_file' function
# to reconfigure the raw Excel data into a cleaned dataset.

# Define the folder containing the Excel files
# and get a list of Excel files in the folder
folder_path = f"{census_raw_files_folder}"
file_list = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.xls')]


for file in file_list:

    if not file.endswith("state_migration_flows_tables.xls"):

        result_df = process_excel_file(file)
        
        # Retrieve the original file name without extension and replace "table" with "dataframe"
        original_file_name = os.path.splitext(os.path.basename(file))[0]
        new_file_name = original_file_name.replace("table", "dataframe") + ".xlsx"
        
        # Define the path which will store the results of the 'process_excel_file' function
        processed_file_path = os.path.join(output_folder, new_file_name)
        
        # Save the processed DataFrame to the designated folder
        result_df.to_excel(processed_file_path, index=False)

        print(f"File '{new_file_name}' saved to '{output_folder}'")