import os
os.environ["PREFECT_API_ENABLE_EPHEMERAL_SERVER"] = "false"

from dotenv import load_dotenv
load_dotenv()

from prefect import flow, task, get_run_logger
import httpx
from httpx import HTTPStatusError, TimeoutException, RequestError
from pathlib import Path
from typing import Any, Dict, List, Union
import psycopg2
import pandas as pd
import json

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
    
    return data

# ---------------------------------------------------------------------------
# Transform – flatten json, extract columsn, rename, and custom calcs
# ---------------------------------------------------------------------------
@task
def flatten_nested_dict(data: Union[Dict, List[Dict]], sep: str = '_') -> pd.DataFrame:
    """
    Use pandas json_normalize with recursive flattening for complete flattening.
    """
    def pre_flatten_lists(obj):
        """Pre-process to flatten list items before json_normalize"""
        if isinstance(obj, dict):
            result = {}
            for key, value in obj.items():
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    # Flatten each dict in the list
                    for i, item in enumerate(value):
                        flattened_item = pre_flatten_lists(item)
                        for sub_key, sub_value in flattened_item.items():
                            result[f"{key}_{i}_{sub_key}"] = sub_value
                elif isinstance(value, dict):
                    # Recursively handle nested dicts
                    flattened_nested = pre_flatten_lists(value)
                    for sub_key, sub_value in flattened_nested.items():
                        result[f"{key}_{sub_key}"] = sub_value
                else:
                    result[key] = value
            return result
        return obj
    
    if isinstance(data, dict):
        flattened = pre_flatten_lists(data)
        return pd.DataFrame([flattened])
    elif isinstance(data, list):
        flattened_list = [pre_flatten_lists(item) for item in data] 
        return pd.DataFrame(flattened_list)

column_mapping = {
    'weather_0_main': 'weather',
    'weather_0_description': 'description',
    'main_temp': 'temperature',
    'main_feels_like': 'feels_like',
    'main_humidity': 'humidity',
    'wind_speed': 'wind_speed',
    'sys_country': 'country',
    'name': 'city'
}

@task 
def extract_rename_columns(df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
    missing_cols = [col for col in column_mapping.keys() if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing columns in DataFrame: {missing_cols}")
    
    return df[list(column_mapping.keys())].rename(columns=column_mapping)


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
            flat_data = flatten_nested_dict(data)
            all_weather_data.append(flat_data)
            
            if all_weather_data:
                combined_df = pd.concat(all_weather_data, ignore_index=True)
                final_data = extract_rename_columns(combined_df, column_mapping)
        
        except Exception as e:
            logger.error(f"Failed to process {city}: {e}")
            continue
    
    print(f"Updated column names for final dataset: {final_data.columns}")


if __name__ == "__main__":
    weather_pipeline.serve(
        name="weather-deployment"
    )