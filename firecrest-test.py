import os
import time
from firecrest import ClientCredentialsAuth
from firecrest.v2 import Firecrest
from prefect import task, flow


@task
def authenticate_and_create_client():
    config = Variable.get("firecrest_demo_config")
    firecrest_url = config['FIRECREST_URL']
    token_uri = config['AUTH_TOKEN_URL']
    system = config['FIRECREST_SYSTEM']

    client_id = Secret.load("firecrest-client-id").get()
    client_secret = Secret.load("firecrest-client-secret").get()

    keycloak = ClientCredentialsAuth(
        client_id, client_secret, token_uri
    )
    
    client = Firecrest(
        firecrest_url=firecrest_url,
        authorization=keycloak
    )
    return client, system


@task
def submit_job(client, system):
    job = client.submit(
        system,
        working_dir="/home/fireuser",
        script_local_path="script.sh"
    )
    return job


@flow
def firecrest_demo_submit():
    client, system = authenticate_and_create_client()
    job = submit_job(client, system)
