"""
Tests for utility functions
"""
import pytest
import os
import sys
import tempfile
import json
from datetime import datetime

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.utils import (
    load_config, save_json, get_date_range, celsius_to_fahrenheit, 
    fahrenheit_to_celsius, format_timestamp, get_weather_icon
)

def test_load_config():
    # Create a temporary config file
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.yaml') as f:
        f.write("""
api:
  url: "https://test.com/api"
  key: "test_key"
        """)
        f.flush()
        
        # Test loading the config
        config = load_config(f.name)
        assert config is not None
        assert 'api' in config
        assert config['api']['url'] == "https://test.com/api"
        assert config['api']['key'] == "test_key"

def test_save_json():
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, 'test.json')
        data = {'test': 'data', 'number': 42}
        
        # Test saving the JSON
        result = save_json(data, filepath)
        assert result is True
        assert os.path.exists(filepath)
        
        # Verify contents
        with open(filepath, 'r') as f:
            loaded_data = json.load(f)
            assert loaded_data == data

def test_get_date_range():
    dates = get_date_range('2023-01-01', '2023-01-05')
    assert len(dates) == 5
    assert dates[0] == '2023-01-01'
    assert dates[-1] == '2023-01-05'

def test_celsius_to_fahrenheit():
    assert celsius_to_fahrenheit(0) == 32
    assert celsius_to_fahrenheit(100) == 212
    assert celsius_to_fahrenheit(-40) == -40

def test_fahrenheit_to_celsius():
    assert fahrenheit_to_celsius(32) == 0
    assert fahrenheit_to_celsius(212) == 100
    assert fahrenheit_to_celsius(-40) == -40

def test_format_timestamp():
    dt = datetime(2023, 1, 1, 12, 0, 0)
    assert format_timestamp(dt) == '2023-01-01 12:00:00'

def test_get_weather_icon():
    assert get_weather_icon('Clear') == '☀️'
    assert get_weather_icon('Rain') == '🌧️'
    assert get_weather_icon('NonExistent') == '❓'
