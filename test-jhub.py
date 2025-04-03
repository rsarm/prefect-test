import requests
import time
from prefect import task, flow, get_run_logger
from prefect.variables import Variable
from prefect.blocks.system import Secret


config = Variable.get("pilatus_config")
JUPYTERHUB_URL = config["url"]
USERNAME = config["user"]
API_TOKEN = Secret.load("pilatus-jhub-token").get()
HEADERS = {"Authorization": f"token {API_TOKEN}"}

user_options = {
    "reservation": [""],  # ["interact"],
    "account": [""],
    "runtime": ["1:00:00"],
    "reservation_custom": [""],
    "uenv": [""],
    "uenv_view": [""],
    "constraint": ["mc"]
}


@task
def start_server(username: str, options: dict) -> None:
    """Starts a JupyterHub server for the given user with custom options."""
    logger = get_run_logger()
    url = f"{JUPYTERHUB_URL}/hub/api/users/{username}/server"
    response = requests.post(url, headers=HEADERS, json=options)

    if response.status_code in [201, 202]:
        logger.info(f"Server spawn request sent for {username} with options.")
    else:
        logger.error(f"Failed to start server: {response.text}")
        raise RuntimeError(f"Failed to start server: {response.text}")

@task(retries=5, retry_delay_seconds=5)
def check_server_status(username: str, attempt: int = 1) -> None:
    """Waits until the server is up, retrying if necessary."""
    logger = get_run_logger()
    url = f"{JUPYTERHUB_URL}/hub/api/users/{username}"
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.warning(f"Request failed: {e}, retrying...")
        raise ValueError("Retrying...")
    
    if data.get("server"):
        logger.info(f"Server is running at {data['server']}")
        return  # Task succeeds
    
    if attempt >= 5:  # If this was the final retry attempt
        logger.error("Server did not start within the allowed retries.")
        raise RuntimeError("Server failed to start.")
    
    logger.warning("Server is not up yet, will retry...")
    raise ValueError("Retrying...")

@task
def stop_server(username: str):
    """Stops the JupyterHub server for the given user."""
    logger = get_run_logger()
    url = f"{JUPYTERHUB_URL}/hub/api/users/{username}/server"
    response = requests.delete(url, headers=HEADERS)

    if response.status_code == 204:
        logger.info(f"Server stopped for {username}.")
    else:
        logger.error(f"Failed to stop server: {response.text}")
        raise RuntimeError(f"Failed to stop server: {response.text}")

@task
def ensure_server_stopped(username: str):
    """Ensures the JupyterHub server is stopped."""
    logger = get_run_logger()
    url = f"{JUPYTERHUB_URL}/hub/api/users/{username}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.warning(f"Request failed while checking stop status: {e}")
        raise RuntimeError("Failed to verify if the server is stopped.")
    
    if data.get("server"):
        logger.error(f"Server for {username} is still running!")
        raise RuntimeError(f"Server for {username} did not stop correctly.")
    logger.info(f"Confirmed: Server for {username} is stopped.")

@flow
def manage_jupyterhub_server():
    """Prefect flow to start, monitor, and stop a JupyterHub server."""
    start_server(USERNAME, user_options)
    check_server_status(USERNAME)
    stop_server(USERNAME)
    ensure_server_stopped(USERNAME)

if __name__ == "__main__":
    manage_jupyterhub_server.serve(name="jhub-submit-pilatus", cron="* * * * *")
