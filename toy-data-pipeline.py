from prefect import task, flow

@task
def fetch_data():
    return "Fetching data"

@task
def transform_data(data):
    return data.upper()

@task
def save_data(data):
    print(f"Data saved: {data}")

@flow
def toy_data_pipeline():
    data = fetch_data()
    transformed_data = transform_data(data)
    save_data(transformed_data)

# Run the flow
if __name__ == "__main__":
    my_data_pipeline.serve(name="toy-data-pipeline", cron="* 5 * * *")
