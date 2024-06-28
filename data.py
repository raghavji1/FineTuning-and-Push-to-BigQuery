import pandas as pd
import json
from dotenv import load_dotenv
from openai import OpenAI

PROMPT = "You are a helpful AI Assistant that is effecient in finding part numbers from unstructured text. Text may not always be in English so deal with that"


def load_and_preprocess_data(file_path):
    df = pd.read_csv(file_path)
    df = df[
        df.apply(
            lambda row: str(row["manufacturer_catalog_number"])
            in str(row["product_name"]),
            axis=1,
        )
    ]
    df = df[["manufacturer_catalog_number", "product_name"]]

    return df


def save_data_to_jsonl(df):
    conversations = []
    for index, row in df.iterrows():
        conversation = {
            "messages": [
                {"role": "system", "content": PROMPT},
                {"role": "user", "content": row["product_name"]},
                {"role": "assistant", "content": row["manufacturer_catalog_number"]},
            ]
        }
        conversations.append(conversation)

    with open("formatted_data.jsonl", "w") as f:
        for entry in conversations:
            f.write(json.dumps(entry) + "\n")



if __name__ == "__main__":
    load_dotenv()
    file_path = "Marcas Part Number.csv"
    data = load_and_preprocess_data(file_path)
    save_data_to_jsonl(data)

    filename = "fine_tuning_data.jsonl"
    upload_file_to_openai(filename)