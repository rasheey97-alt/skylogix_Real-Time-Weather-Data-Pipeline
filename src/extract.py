"""
Weather data extraction module.
 
Workflow:
1. Initialize with configuration from YAML file
2. Connect to OpenWeatherMap API with proper authentication
3. Fetch current weather data for configured cities
4. Add metadata including extraction timestamp
5. Save raw data to JSON files in the raw data directory
6. Return the extracted data for optional further processing
7. Handle API errors with retries and proper logging
"""
import os
import json
import logging
import requests
from datetime import datetime
import pandas as pd
from typing import Dict, List, Any
import yaml
import time
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge

# Load environment variables
load_dotenv()

API_KEY = os.getenv("OPENWEATHERMAP_KEY")
API_KEY = os.getenv("OPENWEATHERMAP_KEY")

if not API_KEY:
    raise ValueError("❌ ERROR: OPENWEATHERMAP_KEY is not set!")
else:
    print(f"✅ API Key Loaded: {API_KEY[:5]}... (hidden for security)")

logger = logging.getLogger(__name__)

# Prometheus metrics
EXTRACT_SUCCESSES = Counter('weather_extract_successes', 'Number of successful API extractions')
EXTRACT_FAILURES = Counter('weather_extract_failures', 'Number of failed API extractions')
API_RESPONSE_TIME = Gauge('weather_api_response_time', 'API response time in seconds')
DATA_POINTS_EXTRACTED = Gauge('weather_data_points_extracted', 'Number of data points extracted')

class WeatherDataExtractor:
    """Class for extracting weather data from OpenWeatherMap API."""
    
    def __init__(self, config_path: str):
        """
        Initialize the extractor with configuration.
        
        Args:
            config_path: Path to the configuration file.
        """
        self.config = self._load_config(config_path)
        self.api_url = self.config['api']['url']
        self.api_key = os.getenv('OPENWEATHERMAP_KEY')
        if not self.api_key:
            self.api_key = self.config['api']['key']
        self.cities = self.config['data']['cities']
        self.timeout = self.config['pipeline']['timeout']
        self.retry_attempts = self.config['pipeline']['retry_attempts']
        self.raw_data_path = self.config['data']['raw_data_path']
        
        # Create raw data directory if it doesn't exist
        os.makedirs(self.raw_data_path, exist_ok=True)
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _make_api_request(self, city: Dict[str, str]) -> Dict[str, Any]:
        """
        Make a request to the OpenWeatherMap API.
        
        Args:
            city: Dictionary containing city name and country code.
            
        Returns:
            Dictionary containing the API response.
        """
        params = {
            'q': f"{city['name']},{city['country']}",
            'appid': self.api_key,
            'units': 'metric'  # Use metric units (Celsius)
        }
        
        for attempt in range(self.retry_attempts):
            try:
                start_time = time.time()
                response = requests.get(
                    self.api_url,
                    params=params,
                    timeout=self.timeout
                )
                response_time = time.time() - start_time
                API_RESPONSE_TIME.set(response_time)
                
                response.raise_for_status()  # Raise error for bad status codes
                EXTRACT_SUCCESSES.inc()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {city['name']}: {str(e)}")
                if attempt == self.retry_attempts - 1:
                    logger.error(f"Failed to fetch data for {city['name']} after {self.retry_attempts} attempts")
                    EXTRACT_FAILURES.inc()
                    return {}
                time.sleep(2)  # Wait before retrying
    
    def extract_current_weather(self) -> List[Dict[str, Any]]:
        """
        Extract current weather data for all configured cities.
        
        Returns:
            List of dictionaries containing weather data for each city.
        """
        weather_data = []
        
        for city in self.cities:
            logger.info(f"Extracting weather data for {city['name']}, {city['country']}")
            
            data = self._make_api_request(city)
            if data:
                # Add timestamp and source information
                data['extraction_timestamp'] = datetime.now().isoformat()
                data['city_name'] = city['name']
                data['country_code'] = city['country']
                
                weather_data.append(data)
                
                # Save raw data
                self._save_raw_data(data, city)
            
        # Update metrics
        DATA_POINTS_EXTRACTED.set(len(weather_data))
        
        return weather_data
    
    def _save_raw_data(self, data: Dict[str, Any], city: Dict[str, str]) -> None:
        """
        Save raw data to JSON file.
        
        Args:
            data: Weather data to save.
            city: City information.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{city['name']}_{city['country']}_{timestamp}.json"
        filepath = os.path.join(self.raw_data_path, filename)
        
        with open(filepath, 'w') as file:
            json.dump(data, file, indent=2)
        
        logger.info(f"Saved raw data to {filepath}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Test the extractor
    extractor = WeatherDataExtractor("./config/config.yaml")
    data = extractor.extract_current_weather()
    print(f"Extracted weather data for {len(data)} cities")