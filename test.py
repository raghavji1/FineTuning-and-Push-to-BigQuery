""" Import Necessary Modules """
import os
import csv
import openai
import tiktoken
import re
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from tqdm import tqdm
load_dotenv()


"""This template extract Part Number and Brand Name"""
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

def fetch_data(df):
    """Fetches notes from the DataFrame."""
    columns = ["Material_Description", "Notes"]
    # merge two columns into one and return it
    queries = df[columns].apply(lambda x: " ".join(x.fillna('')), axis=1).tolist()
    return queries, df["Position"].tolist() 


"""Function Return response of Query """
def process_query(query):
    """Processes a single query using the OpenAI API."""
    chat_response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": template},
            {"role": "user", "content": query},
        ],
    )
    return chat_response.choices[0].message.content


def write_to_file(query, output, filename):
    """Writes a single query and its output to a CSV file."""
    with open(filename, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if os.stat(filename).st_size == 0:  # Check if file is empty
            writer.writerow(["Query", "Output"])
        writer.writerow([query, output])


def remove_extra_whitespaces(text):
    """Removes extra whitespaces between words."""
    return re.sub(r"\s+", " ", text)


def extracting_req_number_and_brand(text, pos):
    """Extracts the required part number and brand name from the text."""
    parts = text.split(";")
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], "Not Found"


"""This logic fetch data from Material Table"""
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


""" count the rows of bigquery materiale table"""
def bigqurey_updated_count_rows():  
    credentials = service_account.Credentials.from_service_account_file('cred.json')
    client = bigquery.Client(credentials=credentials, project='main-411316')
    query = """
    SELECT
        COUNT(*) as row_count
    FROM
        `main-411316.rfq_app.Predictions_Confirmations_copy`
    """
    query_job = client.query(query)
    df = query_job.to_dataframe()
    row_count = df['row_count'][0] 
    return row_count


def insert_into_bigquery(DATA):   
    # Connect to BigQuery
    credentials = service_account.Credentials.from_service_account_file('cred.json')
    client = bigquery.Client(credentials=credentials, project='main-411316') 
    try:
        # Execute the insert query 
        for index, row in DATA.iterrows():
            part_number = row['Part_Number'].replace("'", "''")  # Escaping single quotes
            brand_name = row['ConfirmedBrand'].replace("'", "''")  # Escaping single quotes
            insert_query = f"""
            INSERT INTO `main-411316.rfq_app.Predictions_Confirmations_copy` 
            (Material_ID, RFQ_ID, ConfirmedPartNumber, ConfirmedBrand) 
            VALUES ({row['Material_ID']}, {row['RFQ_ID']}, '{part_number}', '{brand_name}');
            """  
            
            query_job = client.query(insert_query)
            query_job.result()  # Waits for the query to finish
        print("Insert successful!")
    except Exception as e:
        print(f"Error inserting into BigQuery table: {str(e)}")


def test_code_with_5_queries():
    material_bigquery_dataframe = fetch_data_from_bigquery()    
    print('material is executed') 
    count = bigqurey_updated_count_rows() 
    limited_df = material_bigquery_dataframe[:5]  
    print(limited_df)  
    rows, pos = fetch_data(limited_df)  
    print("rows is =", rows, "pos is =", pos)
    # rows = rows[:3]
    results = [] 
    # #filename = "output_from_gpt-3.5-single_number.csv"
    with tqdm(total=len(rows), desc="Processing Queries") as pbar:
        for idx, query in enumerate(rows): 
            query = remove_extra_whitespaces(query)  
            output = process_query(query)  
            
            part_number, brand_name = extracting_req_number_and_brand(output, pos[idx]) 
            results.append({"Query": query, "Part_Number": part_number, "ConfirmedBrand": brand_name})
            pbar.update()   

    results_df = pd.DataFrame(results)  
        
    res = pd.concat([limited_df, results_df], axis=1)  
    print(res) 
    new_df = res[['Material_ID', 'RFQ_ID', 'Part_Number', 'ConfirmedBrand']]
    print("\nNew DataFrame with selected columns:") 
    print(new_df.head()) 
    insert_into_bigquery(new_df) 
    print("executed")

if __name__ == "__main__":
    test_code_with_5_queries()
