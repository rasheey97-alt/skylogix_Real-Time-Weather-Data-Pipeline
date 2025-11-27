"""
Weather data loading module.

Workflow:
1. Load processed data from CSV files in the processed data directory
2. Format the data for appropriate output destinations
3. Load data into different formats and destinations (JSON, CSV, SQLite)
4. Apply database optimizations like indexing for better query performance
5. Generate summary statistics for efficient reporting
6. Handle errors and ensure data integrity during the loading process
7. Return loading status and output file paths
"""
import os
import logging
import pandas as pd
import json
from typing import Dict, List, Any
import yaml
from datetime import datetime
import sqlite3
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)

# Prometheus metrics
LOAD_SUCCESSES = Counter('weather_load_successes', 'Number of successful data loads')
LOAD_FAILURES = Counter('weather_load_failures', 'Number of failed data loads')
RECORDS_LOADED = Gauge('weather_records_loaded', 'Number of records loaded')
LOAD_TIME = Histogram('weather_load_processing_time', 'Time to load data in seconds')

class WeatherDataLoader:
    """Class for loading processed weather data into different formats."""
    
    def __init__(self, config_path: str):
        """
        Initialize the loader with configuration.
        
        Args:
            config_path: Path to the configuration file.
        """
        self.config = self._load_config(config_path)
        self.processed_data_path = self.config['data']['processed_data_path']
        self.output_data_path = self.config['data']['output_data_path']
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_data_path, exist_ok=True)
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _get_latest_processed_file(self) -> str:
        """
        Get the path to the most recent processed data file.
        
        Returns:
            Path to the most recent processed data file.
        """
        import glob
        
        files = glob.glob(os.path.join(self.processed_data_path, "*.csv"))
        if not files:
            logger.warning("No processed data files found")
            return ""
        
        return max(files, key=os.path.getmtime)
    
    @LOAD_TIME.time()
    def load_to_json(self) -> str:
        """
        Load processed data to a JSON file.
        
        Returns:
            Path to the output JSON file, or empty string if failed.
        """
        try:
            latest_file = self._get_latest_processed_file()
            if not latest_file:
                logger.warning("No processed data file found to load")
                LOAD_FAILURES.inc()
                return ""
            
            df = pd.read_csv(latest_file)
            RECORDS_LOADED.set(len(df))
            
            # Convert data to a list of records
            records = df.to_dict(orient='records')
            
            # Create output filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.output_data_path, f"weather_data_{timestamp}.json")
            
            # Write to JSON file
            with open(output_file, 'w') as f:
                json.dump(records, f, indent=2)
            
            logger.info(f"Successfully loaded data to JSON file: {output_file}")
            LOAD_SUCCESSES.inc()
            return output_file
            
        except Exception as e:
            logger.error(f"Error loading data to JSON: {str(e)}")
            LOAD_FAILURES.inc()
            return ""
    
    @LOAD_TIME.time()
    def load_to_sqlite(self, db_path: str = None) -> bool:
        """
        Load processed data to a SQLite database.
        
        Args:
            db_path: Path to the SQLite database file. If None, will create in output directory.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            latest_file = self._get_latest_processed_file()
            if not latest_file:
                logger.warning("No processed data file found to load")
                LOAD_FAILURES.inc()
                return False
            
            if db_path is None:
                db_path = os.path.join(self.output_data_path, "weather_data.db")
            
            df = pd.read_csv(latest_file)
            RECORDS_LOADED.set(len(df))
            
            # Convert timestamp to proper datetime if it exists
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Connect to SQLite database
            conn = sqlite3.connect(db_path)
            
            # Write to database
            df.to_sql('weather_data', conn, if_exists='replace', index=False)
            
            # Create indices for faster queries
            cursor = conn.cursor()
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_city ON weather_data (city)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON weather_data (date)')
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully loaded data to SQLite database: {db_path}")
            LOAD_SUCCESSES.inc()
            return True
            
        except Exception as e:
            logger.error(f"Error loading data to SQLite: {str(e)}")
            LOAD_FAILURES.inc()
            return False
    
    @LOAD_TIME.time()
    def load_to_csv(self, include_summary: bool = True) -> str:
        """
        Load processed data to a formatted CSV file, optionally with summary statistics.
        
        Args:
            include_summary: Whether to include summary statistics in the output.
            
        Returns:
            Path to the output CSV file, or empty string if failed.
        """
        try:
            latest_file = self._get_latest_processed_file()
            if not latest_file:
                logger.warning("No processed data file found to load")
                LOAD_FAILURES.inc()
                return ""
            
            df = pd.read_csv(latest_file)
            RECORDS_LOADED.set(len(df))
            
            # Create output filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.output_data_path, f"weather_report_{timestamp}.csv")
            
            # Create a copy for output
            output_df = df.copy()
            
            # If requested, add summary statistics
            if include_summary:
                # Calculate summary statistics by city
                city_summary = df.groupby('city').agg({
                    'temperature': ['mean', 'min', 'max'],
                    'humidity': ['mean', 'min', 'max'],
                    'wind_speed': ['mean', 'min', 'max']
                }).round(2)
                
                # Save summary to a separate file
                summary_file = os.path.join(self.output_data_path, f"weather_summary_{timestamp}.csv")
                city_summary.to_csv(summary_file)
                logger.info(f"Saved summary statistics to {summary_file}")
            
            # Write to CSV file
            output_df.to_csv(output_file, index=False)
            
            logger.info(f"Successfully loaded data to CSV file: {output_file}")
            LOAD_SUCCESSES.inc()
            return output_file
            
        except Exception as e:
            logger.error(f"Error loading data to CSV: {str(e)}")
            LOAD_FAILURES.inc()
            return ""

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Test the loader
    loader = WeatherDataLoader("./config/config.yaml")
    json_file = loader.load_to_json()
    sqlite_result = loader.load_to_sqlite()
    csv_file = loader.load_to_csv()
    
    print(f"JSON Output: {json_file}")
    print(f"SQLite Result: {sqlite_result}")
    print(f"CSV Output: {csv_file}")