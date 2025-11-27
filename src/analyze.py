"""
Weather data analysis module.

Workflow:
1. Load processed data from CSV files
2. Calculate basic statistics across different dimensions (overall, by city, by date)
3. Analyze temperature trends over time for pattern detection
4. Compare weather metrics across different cities
5. Analyze the distribution of weather conditions
6. Generate visualizations including comparisons, trends, distributions, and correlations
7. Save analysis results and visualizations to the output directory
8. Return comprehensive analysis results as structured data
"""
import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from typing import Dict, List, Any, Tuple
import yaml
from datetime import datetime
import glob
import json
from prometheus_client import Counter, Gauge, Summary
from dotenv import load_dotenv

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
ANALYSIS_SUCCESSES = Counter('weather_analysis_successes', 'Number of successful analyses')
ANALYSIS_FAILURES = Counter('weather_analysis_failures', 'Number of failed analyses')
VISUALIZATIONS_CREATED = Counter('weather_visualizations_created', 'Number of visualizations created')
ANALYSIS_TIME = Summary('weather_analysis_processing_time', 'Analysis processing time in seconds')

class WeatherDataAnalyzer:
    """Class for analyzing and visualizing weather data."""
    
    def __init__(self, config_path: str):
        """
        Initialize the analyzer with configuration.
        
        Args:
            config_path: Path to the configuration file.
        """
        self.config = self._load_config(config_path)
        self.processed_data_path = self.config['data']['processed_data_path']
        self.output_data_path = self.config['data']['output_data_path']
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_data_path, exist_ok=True)
        os.makedirs(os.path.join(self.output_data_path, 'figures'), exist_ok=True)
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _load_processed_data(self) -> pd.DataFrame:
        """
        Load the most recent processed data file.
        
        Returns:
            DataFrame containing processed weather data.
        """
        file_pattern = os.path.join(self.processed_data_path, "*.csv")
        files = glob.glob(file_pattern)
        
        if not files:
            logger.warning("No processed data files found")
            return pd.DataFrame()
        
        # Get the most recent file
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Loading processed data from {latest_file}")
        
        try:
            df = pd.read_csv(latest_file)
            
            # Convert timestamp to datetime
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                
            return df
        
        except Exception as e:
            logger.error(f"Error loading processed data: {str(e)}")
            return pd.DataFrame()
    
    @ANALYSIS_TIME.time()
    def analyze_data(self) -> Dict[str, Any]:
        """
        Analyze the weather data and generate insights.
        
        Returns:
            Dictionary containing analysis results.
        """
        try:
            df = self._load_processed_data()
            
            if df.empty:
                logger.warning("No data available for analysis")
                ANALYSIS_FAILURES.inc()
                return {}
            
            results = {}
            
            # Basic statistics
            results['basic_stats'] = self._calculate_basic_stats(df)
            
            # City comparisons
            results['city_comparisons'] = self._compare_cities(df)
            
            # Temperature trends
            results['temperature_trends'] = self._analyze_temperature_trends(df)
            
            # Weather conditions
            results['weather_conditions'] = self._analyze_weather_conditions(df)
            
            # Save analysis results
            self._save_analysis_results(results)
            
            # Generate visualizations
            self._generate_visualizations(df)
            
            ANALYSIS_SUCCESSES.inc()
            return results
            
        except Exception as e:
            logger.error(f"Error analyzing data: {str(e)}")
            ANALYSIS_FAILURES.inc()
            return {}
    
    def _calculate_basic_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate basic statistics for the weather data.
        
        Args:
            df: DataFrame containing weather data.
            
        Returns:
            Dictionary containing basic statistics.
        """
        stats = {}
        
        # Overall statistics
        stats['overall'] = {
            'count': len(df),
            'date_range': {
                'start': df['timestamp'].min().isoformat() if not df.empty else None,
                'end': df['timestamp'].max().isoformat() if not df.empty else None
            },
            'temperature': {
                'min': df['temperature'].min(),
                'max': df['temperature'].max(),
                'mean': df['temperature'].mean(),
                'median': df['temperature'].median()
            },
            'humidity': {
                'min': df['humidity'].min(),
                'max': df['humidity'].max(),
                'mean': df['humidity'].mean()
            },
            'wind_speed': {
                'min': df['wind_speed'].min(),
                'max': df['wind_speed'].max(),
                'mean': df['wind_speed'].mean()
            }
        }
        
        return stats
    
    def _compare_cities(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compare weather metrics across different cities.
        
        Args:
            df: DataFrame containing weather data.
            
        Returns:
            Dictionary containing city comparisons.
        """
        comparisons = {}
        
        # Group by city
        city_groups = df.groupby('city')
        
        for city, group in city_groups:
            comparisons[city] = {
                'temperature': {
                    'mean': group['temperature'].mean(),
                    'min': group['temperature'].min(),
                    'max': group['temperature'].max()
                },
                'humidity': {
                    'mean': group['humidity'].mean()
                },
                'wind_speed': {
                    'mean': group['wind_speed'].mean()
                }
            }
        
        # Find city with highest and lowest average temperature
        avg_temps = {city: stats['temperature']['mean'] for city, stats in comparisons.items()}
        comparisons['warmest_city'] = max(avg_temps.items(), key=lambda x: x[1])[0]
        comparisons['coldest_city'] = min(avg_temps.items(), key=lambda x: x[1])[0]
        
        return comparisons
    
    def _analyze_temperature_trends(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze temperature trends over time.
        
        Args:
            df: DataFrame containing weather data.
            
        Returns:
            Dictionary containing temperature trend analysis.
        """
        trends = {}
        
        # Only proceed if we have date data
        if 'date' not in df.columns or df.empty:
            return trends
        
        # Resample by date and calculate daily average
        df_daily = df.groupby(['date', 'city']).agg({
            'temperature': 'mean',
            'temp_min': 'min',
            'temp_max': 'max'
        }).reset_index()
        
        # Calculate daily temperature change
        city_groups = df_daily.groupby('city')
        for city, group in city_groups:
            group = group.sort_values('date')
            if len(group) > 1:
                # Calculate daily change
                temp_changes = group['temperature'].diff().dropna().tolist()
                
                trends[city] = {
                    'avg_daily_change': sum(temp_changes) / len(temp_changes) if temp_changes else 0,
                    'max_increase': max(temp_changes) if temp_changes else 0,
                    'max_decrease': min(temp_changes) if temp_changes else 0,
                    'overall_trend': 'warming' if sum(temp_changes) > 0 else 'cooling'
                }
        
        return trends
    
    def _analyze_weather_conditions(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze the distribution of weather conditions.
        
        Args:
            df: DataFrame containing weather data.
            
        Returns:
            Dictionary containing weather condition analysis.
        """
        conditions = {}
        
        # Count weather conditions
        if 'weather_condition' in df.columns:
            condition_counts = df['weather_condition'].value_counts().to_dict()
            conditions['overall_distribution'] = condition_counts
            
            # Most common condition by city
            city_conditions = {}
            city_groups = df.groupby('city')
            
            for city, group in city_groups:
                city_condition_counts = group['weather_condition'].value_counts()
                if not city_condition_counts.empty:
                    city_conditions[city] = city_condition_counts.index[0]
            
            conditions['most_common_by_city'] = city_conditions
        
        return conditions
    
    def _save_analysis_results(self, results: Dict[str, Any]) -> None:
        """
        Save analysis results to JSON file.
        
        Args:
            results: Dictionary containing analysis results.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"weather_analysis_{timestamp}.json"
        filepath = os.path.join(self.output_data_path, filename)
        
        with open(filepath, 'w') as file:
            json.dump(results, file, indent=2)
        
        logger.info(f"Saved analysis results to {filepath}")
    
    def _generate_visualizations(self, df: pd.DataFrame) -> None:
        """
        Generate visualizations for the weather data.
        
        Args:
            df: DataFrame containing weather data.
        """
        if df.empty:
            logger.warning("No data available for visualizations")
            return
        
        # Set up plot style
        plt.style.use('ggplot')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Temperature comparison across cities
        self._plot_city_temperature_comparison(df, timestamp)
        
        # 2. Temperature trends over time
        self._plot_temperature_trends(df, timestamp)
        
        # 3. Weather condition distribution
        self._plot_weather_condition_distribution(df, timestamp)
        
        # 4. Correlation between temperature and humidity
        self._plot_temperature_humidity_correlation(df, timestamp)
        
        # 5. Wind speed comparison
        self._plot_wind_speed_comparison(df, timestamp)
        
    def _plot_city_temperature_comparison(self, df: pd.DataFrame, timestamp: str) -> None:
        """Plot temperature comparison across cities."""
        try:
            plt.figure(figsize=(12, 6))
            
            city_temp = df.groupby('city')['temperature'].mean().sort_values(ascending=False)
            city_temp.plot(kind='bar', color='skyblue')
            
            plt.title('Average Temperature by City')
            plt.ylabel('Temperature (°C)')
            plt.xlabel('City')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            filename = f"city_temperature_comparison_{timestamp}.png"
            filepath = os.path.join(self.output_data_path, 'figures', filename)
            plt.savefig(filepath)
            plt.close()
            
            logger.info(f"Saved city temperature comparison plot to {filepath}")
            VISUALIZATIONS_CREATED.inc()
        except Exception as e:
            logger.error(f"Error generating city temperature comparison plot: {str(e)}")
    
    def _plot_temperature_trends(self, df: pd.DataFrame, timestamp: str) -> None:
        """Plot temperature trends over time."""
        try:
            if 'date' not in df.columns or df.empty:
                return
            
            plt.figure(figsize=(14, 7))
            
            # Group by date and city, calculate mean temperature
            daily_temps = df.groupby(['date', 'city'])['temperature'].mean().reset_index()
            
            # Pivot to have cities as columns
            pivot_temps = daily_temps.pivot(index='date', columns='city', values='temperature')
            
            # Plot line for each city
            pivot_temps.plot(marker='o', linestyle='-', alpha=0.7, figsize=(14, 7))
            
            plt.title('Temperature Trends by City')
            plt.ylabel('Temperature (°C)')
            plt.xlabel('Date')
            plt.grid(True, alpha=0.3)
            plt.legend(title='City')
            plt.tight_layout()
            
            filename = f"temperature_trends_{timestamp}.png"
            filepath = os.path.join(self.output_data_path, 'figures', filename)
            plt.savefig(filepath)
            plt.close()
            
            logger.info(f"Saved temperature trends plot to {filepath}")
            VISUALIZATIONS_CREATED.inc()
        except Exception as e:
            logger.error(f"Error generating temperature trends plot: {str(e)}")
    
    def _plot_weather_condition_distribution(self, df: pd.DataFrame, timestamp: str) -> None:
        """Plot weather condition distribution."""
        try:
            if 'weather_condition' not in df.columns:
                return
            
            plt.figure(figsize=(10, 6))
            
            condition_counts = df['weather_condition'].value_counts()
            condition_counts.plot(kind='pie', autopct='%1.1f%%', colors=plt.cm.tab10.colors)
            
            plt.title('Distribution of Weather Conditions')
            plt.ylabel('')  # Hide the ylabel
            plt.tight_layout()
            
            filename = f"weather_condition_distribution_{timestamp}.png"
            filepath = os.path.join(self.output_data_path, 'figures', filename)
            plt.savefig(filepath)
            plt.close()
            
            logger.info(f"Saved weather condition distribution plot to {filepath}")
            VISUALIZATIONS_CREATED.inc()
        except Exception as e:
            logger.error(f"Error generating weather condition distribution plot: {str(e)}")
            
    def _plot_temperature_humidity_correlation(self, df: pd.DataFrame, timestamp: str) -> None:
        """Plot correlation between temperature and humidity."""
        try:
            if 'temperature' not in df.columns or 'humidity' not in df.columns:
                return
                
            plt.figure(figsize=(10, 6))
            
            # Scatter plot with color by city
            cities = df['city'].unique()
            for city in cities:
                city_data = df[df['city'] == city]
                plt.scatter(
                    city_data['temperature'], 
                    city_data['humidity'],
                    alpha=0.6,
                    label=city
                )
            
            # Add trend line for all data
            z = np.polyfit(df['temperature'], df['humidity'], 1)
            p = np.poly1d(z)
            plt.plot(
                df['temperature'].sort_values(), 
                p(df['temperature'].sort_values()),
                "r--", 
                alpha=0.8,
                label="Trend Line"
            )
            
            plt.title('Temperature vs. Humidity')
            plt.xlabel('Temperature (°C)')
            plt.ylabel('Humidity (%)')
            plt.grid(True, alpha=0.3)
            plt.legend(title='City')
            plt.tight_layout()
            
            filename = f"temperature_humidity_correlation_{timestamp}.png"
            filepath = os.path.join(self.output_data_path, 'figures', filename)
            plt.savefig(filepath)
            plt.close()
            
            logger.info(f"Saved temperature-humidity correlation plot to {filepath}")
            VISUALIZATIONS_CREATED.inc()
        except Exception as e:
            logger.error(f"Error generating temperature-humidity correlation plot: {str(e)}")
            
    def _plot_wind_speed_comparison(self, df: pd.DataFrame, timestamp: str) -> None:
        """Plot wind speed comparison across cities."""
        try:
            plt.figure(figsize=(12, 6))
            
            # Create box plot for wind speed by city
            df.boxplot(column='wind_speed', by='city', figsize=(12, 6))
            
            plt.title('Wind Speed Distribution by City')
            plt.suptitle('')  # Remove default suptitle
            plt.ylabel('Wind Speed (m/s)')
            plt.xlabel('City')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            filename = f"wind_speed_comparison_{timestamp}.png"
            filepath = os.path.join(self.output_data_path, 'figures', filename)
            plt.savefig(filepath)
            plt.close()
            
            logger.info(f"Saved wind speed comparison plot to {filepath}")
            VISUALIZATIONS_CREATED.inc()
        except Exception as e:
            logger.error(f"Error generating wind speed comparison plot: {str(e)}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Test the analyzer
    analyzer = WeatherDataAnalyzer("./config/config.yaml")
    results = analyzer.analyze_data()
    print(f"Analysis complete with {len(results)} result categories")
