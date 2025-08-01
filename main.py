import os
os.environ["PREFECT_API_ENABLE_EPHEMERAL_SERVER"] = "false"

from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task, get_run_logger
import httpx
from httpx import HTTPStatusError, TimeoutException, RequestError
from pathlib import Path
import psycopg2
import pandas as pd
import json

# Load environment variables


# Get environment variables
api_key = os.getenv("OPENWEATHERMAP_API_KEY")
base_url = os.getenv("API_BASE_URL")

# Debug: Check if environment variables are loaded
print(f"Currnet working directory: {os.getcwd}")
print(f"Looking for .env file: {Path('.env').absolute()}")
print(f".env file exists: {Path('.env').exists()}")
print(f"API Key loaded: {'Yes' if api_key else 'No'}")
print(f"API Key (first 10 chars): {api_key[:10] if api_key else 'None'}...")
print(f"Base URL: {base_url}")

# Validate required environment varibles
if not api_key:
    raise ValueError("Missing OPENWEATHERMAP API Key in .env file")

if not base_url:
    raise ValueError("Missing API_BASE_URL in .env file")

# ---------------------------------------------------------------------------
# Extract – fetch weather data from OpenWeatherMap API
# ---------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=10)
def get_current_weather(city: str, api_key: str, units: str = 'metric') -> dict:
    """Fech current weather data from a given city using OpenWeatherMap API
    
        Args:
            city: City name (e.g., "London" or "London, UK")
            api_key: OpenWeatherMap API key
            units: Temperature units('metric', 'imperial', or 'standard')
        
        Returns:
            dict: Weather data from OpenWeatherMap API
    """
    # Construct the API URL
    url = f"{base_url}/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": units
    }

    logger = get_run_logger()
    logger.info(f"Fetching weather data for {city}")
    logger.info(f"API URL: {url}")
    logger.info(f"Parameters: {json.dumps({k: v if k != 'appid' else f'{v[:10]}...' for k, v in params.items()})}")


    try:
        response = httpx.get(url, params=params, timeout=30)

        # Log the full URL for debuggin
        logger.info(f"Full request URL: {response.url}")

        # Check for HTTP errors
        response.raise_for_status()

        # Parse JSON response
        data = response.json()

        # Check if the API returned an error in the JSON
        if data.get('cod') != 200:
            error_msg = data.get('message', 'Unknown API error')
            logger.error(f"API returned error code {data.get('cod')}")
            raise Exception(f"OpenWeatherMap API error: {error_msg}")
        
        logger.info(f"Successfully retrieved weather data for {city}")
        return data
    
    except HTTPStatusError as e:
        logger.error(f"HTTP error for {city}: {e.response.status_code}")
        logger.error(f"Response text: {e.response.text}")
        raise

    except TimeoutException as e:
        logger.error(f"Timeout while fetching weather for {city}")
        raise

    except RequestError as e:
        logger.error(f" Request error for {city}: {str(e)}")
        raise

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response for {city}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error for {city}: {str(e)}")
        raise

# ---------------------------------------------------------------------------
# Transform – flatten json, extract columsn, rename, and custom calcs
# ---------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=30)
def clean_weather_data(weather_data: dict) -> dict:
    pass

@flow(name="Weather ETL Pipeline")
def weather_pipeline():
    """Main ETL pipeline for weather data"""
    logger = get_run_logger()

    # List of cities to getch weather for
    cities = ["London", "New York", "Tokyo", "Sydney"]

    all_weather_data = []
    
    for city in cities:
        try:
            data = get_current_weather(city, api_key)
            all_weather_data.append(data)

        except Exception as e:
            logger.error(f"Failed to process {city}: {e}")
            continue



if __name__ == "__main__":
    weather_pipeline.serve(
        name="weather-deployment"
    )