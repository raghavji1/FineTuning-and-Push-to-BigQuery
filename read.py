import os
import csv
import openai
import re
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from tqdm import tqdm
import time

load_dotenv()

# Template for the GPT-3.5 model
template = """
        You are a helpful AI assistant skilled in extracting part numbers of a product from given unstructured text. The text may not be in English so you have to deal with it. Also, there can be SKU part numbers and Company part numbers you have to extract the company part number from the text. Also, please keep in mind that if you can't find the part number in the text, return 'cant find the part number' and when you find a part number you should return it as it is without any extra text. Note the part numbers do not contain WI with them.
        Additionally, you have to extract Brand Name from the given inputs, if the brand name is available in the above statement, you return the brand name otherwise you will return "Not Found".
        Here are some examples of how the input would look like and part numbers and brand names present in the text:

        Examples:
        Input: CONTACTOR,VAC,SIEMENS 3RT12766NF36 Processor,Communications,Model cp 342-5 Contactor Modifier 1 Vacuum Additional modifiers 250kW Additional modifiers 400V Additional modifiers 500A Additional modifiers 3 pole Additional modifiers 2 n/o,2 n/c Contactor Modifier 1 Vacuum Additional modifiers 250kW Additional modifiers 400V Additional modifiers 500A Additional modifiers 3 pole Additional modifiers 2 n/o,2 n/c
        Output: 3RT12766NF36;SIEMENS

        Input: RUBBER,MAGIC,NABUSPA NBSMR192 https://www.raytech.it/es/producto/baja-tension/rellenos/gomas/magic-rubber Goma bicomponente de reticulación rápida. Goma bicomponente líquida aislante, extremadamente conformante y envolvente, flexible, elástica y reaccesible. En pocos minutos se transforma en una goma de muy elevadas característ
        Output: NBSMR192;NABUSPA

        Input: CHAQUETA,UTILIDAD,PESADO,XL,88% ALGODON, 11454194 CHAQUETA,UTILIDAD,PESADO,XL,88% ALGODON, Numero de parte: CLC4-XL-U Chaqueta,Utilidad Short Name Chaqueta,Utilidad Estilo De La Chaqueta Pesado Talla De La Chaqueta XL Material De La Chaqueta 88 algodon,12% nailon JACKET COLOUR 
        Output: CLC4-XL-U;Not Found
        """

"""
So, the function is taking the contents of the "Material_Description" and "Notes" columns from each row in the CSV file, combining them into one string, and keeping track of their positions.
So, when you call the fetch_data function, you receive these two lists as output: one containing the combined text pieces and the other containing their corresponding positions.
"""

# Fetches and combines the contents of "Material_Description" and "Notes" columns from the DataFrame
def fetch_data(df):
    columns = ["Material_Description", "Notes"]
    queries = df[columns].apply(lambda x: " ".join(x.fillna('')), axis=1).tolist()
    return queries, df["Position"].tolist()


def process_query(query):
    """Processes a single query using the OpenAI API."""
    chat_response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        x
        ],
    )
    return chat_response.choices[0].message.content

# Writes a single query and its output to a CSV file
def write_to_file(query, output, filename):
    with open(filename, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if os.stat(filename).st_size == 0:  # Check if file is empty
            writer.writerow(["Query", "Output"])
        writer.writerow([query, output])

# Removes extra whitespaces between words
def remove_extra_whitespaces(text):
    return re.sub(r"\s+", " ", text)

# Extracts the required part number and brand name from the text
def extracting_req_number_and_brand(text, pos):
    parts = text.split(";")
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], "Not Found"

# Fetches data from BigQuery table
def fetch_data_from_bigquery():
    credentials = service_account.Credentials.from_service_account_file('cred.json')
    client = bigquery.Client(credentials=credentials, project='main-411316')
    query = """
     SELECT
         * 
     FROM
         `main-411316.rfq_scraper_dataset.Material`
     """
    query_job = client.query(query)
    df = query_job.to_dataframe()
    return df

# Gets the row count of the Predictions_Confirmations_copy table
def bigquery_updated_count_rows():
    credentials = service_account.Credentials.from_service_account_file('cred.json')
    client = bigquery.Client(credentials=credentials, project='main-411316')
    query = """
    SELECT
        COUNT(*) as row_count
    FROM
        `main-411316.rfq_app.Predictions_Confirmations`
    """
    query_job = client.query(query)
    df = query_job.to_dataframe()
    row_count = df['row_count'][0]
    return row_count

# Inserts data into BigQuery table
def insert_into_bigquery(DATA):
    credentials = service_account.Credentials.from_service_account_file('cred.json')
    client = bigquery.Client(credentials=credentials, project='main-411316')
    try:
        for index, row in DATA.iterrows():
            part_number = str(row['Part_Number']).replace("'", "''")  # Escaping single quotes
            brand_name = str(row['ConfirmedBrand']).replace("'", "''")  # Escaping single quotes
            insert_query = f"""
            INSERT INTO `main-411316.rfq_app.Predictions_Confirmations`
            (Material_ID, RFQ_ID, ConfirmedPartNumber, ConfirmedBrand)
            VALUES ({row['Material_ID']}, {row['RFQ_ID']}, '{part_number}', '{brand_name}');
            """
            query_job = client.query(insert_query)
            query_job.result()  # Waits for the query to finish
        print("Insert successful!")
    except Exception as e:
        print(f"Error inserting into BigQuery table: {str(e)}")

# Retrieves the last processed Material_ID from a file
def get_last_processed_material_id(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return int(file.read().strip())
    return None

# Saves the last processed Material_ID to a file
def save_last_processed_material_id(material_id, filename):
    with open(filename, 'w') as file:
        file.write(str(material_id))

# Processes a chunk of data
def process_chunk(chunk):
    rows, pos = fetch_data(chunk)
    results = []
    with tqdm(total=len(rows), desc="Processing Queries") as pbar:
        for idx, query in enumerate(rows):
            query = remove_extra_whitespaces(query)
            output = process_query(query)
            part_number, brand_name = extracting_req_number_and_brand(output, pos[idx])
            results.append({
                "Material_ID": chunk['Material_ID'].iloc[idx],
                "RFQ_ID": chunk['RFQ_ID'].iloc[idx],
                "Part_Number": part_number,
                "ConfirmedBrand": brand_name
            })
            pbar.update()
    return results

# Main function to run the entire process in a loop
def main(chunk_size=100):
    while True:
        material_bigquery_dataframe = fetch_data_from_bigquery()  # Fetches data from BigQuery
        print('Material data fetched')

        last_processed_material_id = get_last_processed_material_id('last_inserted_index.txt')
        
        if last_processed_material_id:
            # Filter new data based on last processed Material_ID
            new_data = material_bigquery_dataframe[material_bigquery_dataframe['Material_ID'] > last_processed_material_id]
        else:
            new_data = material_bigquery_dataframe
        
        if new_data.empty:
            print("No new data to process.")
        else:
            print('New data available for processing')

            # Process data in chunks
            for start in range(0, len(new_data), chunk_size):
                end = start + chunk_size
                chunk = new_data[start:end]
                results = process_chunk(chunk)
                results_df = pd.DataFrame(results)
                new_df = results_df[['Material_ID', 'RFQ_ID', 'Part_Number', 'ConfirmedBrand']]
                
                if not new_df.empty:
                    # Save the last processed Material_ID
                    save_last_processed_material_id(new_df['Material_ID'].iloc[-1], 'last_inserted_index.txt')
                    # Insert the processed data into BigQuery
                    insert_into_bigquery(new_df)
        
        print("Execution completed. Waiting for next interval...")
          # Wait for the specified interval before the next run

if __name__ == "__main__":
    main(chunk_size=100)  # Run the process every 30 minutes with a chunk size of 100
