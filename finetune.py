from openai import OpenAI
from dotenv import load_dotenv


def upload_file_to_openai(filename):
    client = OpenAI()

    obj = client.files.create(
        file=open(filename, "rb"),
        purpose="fine-tune",
    )

    return obj


def create_finetune_job(obj):
    client = OpenAI()

    job = client.fine_tuning.jobs.create(
        training_file=obj.id,
        model="gpt-3.5-turbo",
        hyperparameters={"n_epochs": 20, "batch_size": 50},
        suffix="RFQv0.1"
    )

    return job


if __name__ == "__main__":
    load_dotenv()
    #filename = "fine_tuning_data.jsonl"
    #obj = upload_file_to_openai(filename)
    #job = create_finetune_job(obj)
    #print(job)
    client = OpenAI()
    files = client.files.list()
    jobs = client.fine_tuning.jobs.list()
    #print(files)
    #print(jobs)
    print(client.fine_tuning.jobs.retrieve("ftjob-VHirkkv7SuMsOWS4LxdbytMm"))
    # print(client.files.retrieve_content('file-PeBSNIetzrAeMMKMvRWSyJiY'))