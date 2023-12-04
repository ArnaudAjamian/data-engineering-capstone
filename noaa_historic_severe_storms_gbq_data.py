from google.cloud import bigquery
import concurrent.futures

from credentials import project_id, file_path


client = bigquery.Client(project = project_id)


years = range(1950, 2024)

# Export and save function will retrieve data from the public dataset
# and store the results as CSV files within the designate file path
def export_and_save(years):

    table_id = f"storms_{years}"

    query = f"SELECT * FROM `bigquery-public-data.noaa_historic_severe_storms.{table_id}`"

    # Execute the query
    query_job = client.query(query)

    # Save the query results into a dataframe
    df = query_job.to_dataframe()

    # Save the dataframe as a CSV file
    output_file_path = f"{file_path}/{table_id}.csv"
    df.to_csv(output_file_path, index = False)

    print(f"CSV file saved at: {output_file_path}")



with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(export_and_save, years)