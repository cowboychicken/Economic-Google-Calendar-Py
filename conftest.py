import pytest
from dotenv import load_dotenv
import os

@pytest.fixture(scope="session", autouse=True)
def load_env():
    """
    A pytest fixture that automatically loads the .env file
    before any tests run.
    """
    # Find the path to the .env file (it's in the same dir as this file)
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    
    if os.path.exists(env_path):
        print(f"\n--- Loading environment from {env_path} ---")
        load_dotenv(dotenv_path=env_path)
    else:
        print(f"\n--- .env file not found at {env_path}, skipping load ---")
        