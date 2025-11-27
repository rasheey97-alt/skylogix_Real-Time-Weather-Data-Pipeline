"""
Utility functions for the weather data pipeline.

Workflow:
1. Provide helper functions used across the pipeline
2. Handle configuration loading and validation
3. Offer file operations and JSON serialization
4. Implement date and time handling utilities
5. Provide data conversion functions
6. Supply weather-specific utilities like condition icons
7. Support monitoring and instrumentation
"""
import os
import logging
import yaml
import json
from typing import Dict, Any, List
from datetime import datetime, timedelta
import time
from prometheus_client import start_http_server

logger = logging.getLogger(__name__)

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file.
        
    Returns:
        Dictionary containing configuration parameters.
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logger.error(f"Error loading configuration file: {str(e)}")
        return {}

def save_json(data: Dict[str, Any], filepath: str) -> bool:
    """
    Save data to JSON file.
    
    Args:
        data: Data to save.
        filepath: Path to save the file.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w') as file:
            json.dump(data, file, indent=2)
        
        logger.info(f"Saved data to {filepath}")
        return True
    
    except Exception as e:
        logger.error(f"Error saving JSON file: {str(e)}")
        return False

def get_date_range(start_date: str, end_date: str) -> List[str]:
    """
    Get a list of dates between start_date and end_date (inclusive).
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format.
        end_date: End date in 'YYYY-MM-DD' format.
        
    Returns:
        List of date strings in 'YYYY-MM-DD' format.
    """
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        date_list = []
        current = start
        
        while current <= end:
            date_list.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return date_list
    
    except Exception as e:
        logger.error(f"Error generating date range: {str(e)}")
        return []

def celsius_to_fahrenheit(celsius: float) -> float:
    """
    Convert temperature from Celsius to Fahrenheit.
    
    Args:
        celsius: Temperature in Celsius.
        
    Returns:
        Temperature in Fahrenheit.
    """
    return (celsius * 9/5) + 32

def fahrenheit_to_celsius(fahrenheit: float) -> float:
    """
    Convert temperature from Fahrenheit to Celsius.
    
    Args:
        fahrenheit: Temperature in Fahrenheit.
        
    Returns:
        Temperature in Celsius.
    """
    return (fahrenheit - 32) * 5/9

def format_timestamp(timestamp: datetime) -> str:
    """
    Format datetime object to string.
    
    Args:
        timestamp: Datetime object.
        
    Returns:
        Formatted timestamp string.
    """
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')

def get_weather_icon(condition: str) -> str:
    """
    Get a simple text icon for a weather condition.
    
    Args:
        condition: Weather condition.
        
    Returns:
        Text icon representing the condition.
    """
    icons = {
        'Clear': '☀️',
        'Clouds': '☁️',
        'Rain': '🌧️',
        'Drizzle': '🌦️',
        'Thunderstorm': '⛈️',
        'Snow': '❄️',
        'Mist': '🌫️',
        'Fog': '🌫️',
        'Haze': '🌫️',
        'Smoke': '🌫️',
        'Dust': '🌫️',
        'Sand': '🌫️',
        'Ash': '🌫️',
        'Squall': '💨',
        'Tornado': '🌪️'
    }
    
    return icons.get(condition, '❓')

def setup_monitoring(config: Dict[str, Any]) -> None:
    """
    Set up Prometheus monitoring server.
    
    Args:
        config: Configuration dictionary.
    """
    try:
        port = config['monitoring']['prometheus_port']
        logger.info(f"Starting Prometheus metrics server on port {port}")
        start_http_server(port)
    except Exception as e:
        logger.error(f"Error setting up monitoring: {str(e)}")