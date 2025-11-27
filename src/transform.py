"""
Weather data transformation module.

Workflow:
1. Load raw JSON data files from the raw data directory
2. Extract relevant weather features from the complex JSON structure
3. Standardize data formats and handle missing values
4. Detect and handle outliers using statistical methods
5. Create derived features for enhanced analysis
6. Standardize timestamps and ensure consistent data types
7. Save the processed data as CSV files for further analysis
8. Return the transformed DataFrame for optional further processing
"""
import os
import json
import logging
import pandas as pd
from typing import Dict, List, Any
import yaml
from datetime import datetime
import glob
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)

# Prometheus metrics
TRANSFORM_SUCCESSES = Counter('weather_transform_successes', 'Number of successful transformations')
TRANSFORM_FAILURES = Counter('weather_transform_failures', 'Number of failed transformations')
FILES_PROCESSED = Gauge('weather_files_processed', 'Number of files processed')
RECORDS_PROCESSED = Gauge('weather_records_processed', 'Number of records processed')
OUTLIERS_DETECTED = Counter('weather_outliers_detected', 'Number of outliers detected and handled')
PROCESSING_TIME = Histogram('weather_transform_processing_time', 'Time to process data in seconds')

class WeatherDataTransformer:
    """Class for transforming and cleaning weather data."""
    
    def __init__(self, config_path: str):
        """
        Initialize the transformer with configuration.
        
        Args:
            config_path: Path to the configuration file.
        """
        self.config = self._load_config(config_path)
        self.raw_data_path = self.config['data']['raw_data_path']
        self.processed_data_path = self.config['data']['processed_data_path']
        
        # Create processed data directory if it doesn't exist
        os.makedirs(self.processed_data_path, exist_ok=True)
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _load_raw_data_files(self) -> List[Dict[str, Any]]:
        """
        Load all raw data files from the raw data directory.
        
        Returns:
            List of dictionaries containing raw weather data.
        """
        raw_data = []
        file_pattern = os.path.join(self.raw_data_path, "*.json")
        
        files = glob.glob(file_pattern)
        FILES_PROCESSED.set(len(files))
        
        for file_path in files:
            try:
                with open(file_path, 'r') as file:
                    data = json.load(file)
                    raw_data.append(data)
            except Exception as e:
                logger.error(f"Error loading file {file_path}: {str(e)}")
        
        logger.info(f"Loaded {len(raw_data)} raw data files")
        return raw_data
    
    def _extract_weather_features(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Extract relevant features from raw weather data.
        
        Args:
            raw_data: List of dictionaries containing raw weather data.
            
        Returns:
            DataFrame containing transformed weather data.
        """
        weather_records = []
        
        for record in raw_data:
            try:
                # Check if record has required fields
                if not all(key in record for key in ['main', 'wind', 'weather', 'city_name', 'country_code']):
                    logger.warning(f"Skipping record with missing keys: {record.get('city_name', 'Unknown')}")
                    continue
                
                # Extract basic information
                city_name = record['city_name']
                country_code = record['country_code']
                
                # Extract timestamp or use extraction timestamp
                if 'dt' in record:
                    timestamp = datetime.fromtimestamp(record['dt'])
                else:
                    timestamp = datetime.fromisoformat(record['extraction_timestamp'])
                
                # Extract temperature data
                temperature = record['main'].get('temp', None)
                feels_like = record['main'].get('feels_like', None)
                temp_min = record['main'].get('temp_min', None)
                temp_max = record['main'].get('temp_max', None)
                pressure = record['main'].get('pressure', None)
                humidity = record['main'].get('humidity', None)
                
                # Extract wind data
                wind_speed = record['wind'].get('speed', None)
                wind_direction = record['wind'].get('deg', None)
                
                # Extract weather condition
                weather_main = record['weather'][0].get('main', None) if record['weather'] else None
                weather_description = record['weather'][0].get('description', None) if record['weather'] else None
                
                # Create a clean record
                clean_record = {
                    'city': city_name,
                    'country': country_code,
                    'timestamp': timestamp,
                    'temperature': temperature,
                    'feels_like': feels_like,
                    'temp_min': temp_min,
                    'temp_max': temp_max,
                    'pressure': pressure,
                    'humidity': humidity,
                    'wind_speed': wind_speed,
                    'wind_direction': wind_direction,
                    'weather_condition': weather_main,
                    'weather_description': weather_description
                }
                
                weather_records.append(clean_record)
                
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
        
        # Convert to DataFrame
        df = pd.DataFrame(weather_records)
        RECORDS_PROCESSED.set(len(df))
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the data by handling missing values and outliers.
        
        Args:
            df: DataFrame containing weather data.
            
        Returns:
            Cleaned DataFrame.
        """
        # Make a copy to avoid modifying the original
        cleaned_df = df.copy()
        
        outlier_count = 0
        
        # Handle missing values
        numeric_columns = ['temperature', 'feels_like', 'temp_min', 'temp_max',
                          'pressure', 'humidity', 'wind_speed', 'wind_direction']
        
        for col in numeric_columns:
            # Replace extreme outliers with NaN
            if col in cleaned_df.columns:
                q1 = cleaned_df[col].quantile(0.05)
                q3 = cleaned_df[col].quantile(0.95)
                iqr = q3 - q1
                lower_bound = q1 - (1.5 * iqr)
                upper_bound = q3 + (1.5 * iqr)
                
                # Count outliers
                outliers = ((cleaned_df[col] < lower_bound) | (cleaned_df[col] > upper_bound)).sum()
                outlier_count += outliers
                
                # Replace outliers with NaN
                cleaned_df.loc[(cleaned_df[col] < lower_bound) | (cleaned_df[col] > upper_bound), col] = None
                
                # Fill NaN values with median for numeric columns
                cleaned_df[col] = cleaned_df[col].fillna(cleaned_df[col].median())
        
        # Update outlier metrics
        OUTLIERS_DETECTED.inc(outlier_count)
        
        # Fill missing categorical values
        cleaned_df['weather_condition'] = cleaned_df['weather_condition'].fillna('Unknown')
        cleaned_df['weather_description'] = cleaned_df['weather_description'].fillna('Unknown')
        
        # Add derived features
        cleaned_df['date'] = cleaned_df['timestamp'].dt.date
        cleaned_df['hour'] = cleaned_df['timestamp'].dt.hour
        cleaned_df['day_of_week'] = cleaned_df['timestamp'].dt.day_name()
        
        # Temperature difference (daily range)
        cleaned_df['temp_range'] = cleaned_df['temp_max'] - cleaned_df['temp_min']
        
        return cleaned_df
    
    def transform_data(self) -> pd.DataFrame:
        """
        Main method to transform raw weather data.
        
        Returns:
            DataFrame containing transformed and cleaned weather data.
        """
        with PROCESSING_TIME.time():
            try:
                # Load raw data
                raw_data = self._load_raw_data_files()
                
                if not raw_data:
                    logger.warning("No raw data files found to transform")
                    TRANSFORM_FAILURES.inc()
                    return pd.DataFrame()
                
                # Extract features
                df = self._extract_weather_features(raw_data)
                
                if df.empty:
                    logger.warning("No data extracted from raw files")
                    TRANSFORM_FAILURES.inc()
                    return pd.DataFrame()
                
                # Clean data
                cleaned_df = self._clean_data(df)
                
                # Save processed data
                self._save_processed_data(cleaned_df)
                
                TRANSFORM_SUCCESSES.inc()
                return cleaned_df
                
            except Exception as e:
                logger.error(f"Error transforming data: {str(e)}")
                TRANSFORM_FAILURES.inc()
                return pd.DataFrame()
    
    def _save_processed_data(self, df: pd.DataFrame) -> None:
        """
        Save processed data to CSV file.
        
        Args:
            df: DataFrame containing processed weather data.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"processed_weather_data_{timestamp}.csv"
        filepath = os.path.join(self.processed_data_path, filename)
        
        df.to_csv(filepath, index=False)
        logger.info(f"Saved processed data to {filepath}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Test the transformer
    transformer = WeatherDataTransformer("./config/config.yaml")
    processed_data = transformer.transform_data()
    print(f"Transformed data shape: {processed_data.shape}")