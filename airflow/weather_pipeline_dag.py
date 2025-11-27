"""
Weather Data Pipeline DAG for Apache Airflow

Workflow:
1. Extract weather data from OpenWeatherMap API
2. Transform and clean the extracted data
3. Analyze the data and generate visualizations
4. Load the data into various formats
5. Monitor pipeline execution and send notifications
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
import os
import requests
import json
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Weather Data ETL Pipeline',
    schedule_interval='0 0 * * *',  # Run at midnight every day
    catchup=False,
    tags=['weather', 'etl', 'data-engineering'],
)

# Get OpenWeatherMap API key from Airflow Variables
api_key = Variable.get("openweathermap_api_key", default_var="your_api_key_here")

# Define cities to fetch data for
cities = [
    {"name": "New York", "country": "US"},
    {"name": "London", "country": "GB"},
    {"name": "Tokyo", "country": "JP"},
    {"name": "Sydney", "country": "AU"},
    {"name": "Berlin", "country": "DE"}
]

def extract_weather_data(**kwargs):
    """Extract weather data from OpenWeatherMap API"""
    logger = logging.getLogger("airflow.task")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = []
    
    # Create directories if they don't exist
    os.makedirs("/tmp/weather_data/raw", exist_ok=True)
    
    for city in cities:
        try:
            logger.info(f"Extracting data for {city['name']}, {city['country']}")
            
            params = {
                'q': f"{city['name']},{city['country']}",
                'appid': api_key,
                'units': 'metric'
            }
            
            response = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            # Add metadata
            data['extraction_timestamp'] = datetime.now().isoformat()
            data['city_name'] = city['name']
            data['country_code'] = city['country']
            
            # Save raw data
            filename = f"{city['name']}_{city['country']}_{timestamp}.json"
            filepath = f"/tmp/weather_data/raw/{filename}"
            
            with open(filepath, 'w') as file:
                json.dump(data, file, indent=2)
            
            results.append({
                'city': city['name'],
                'status': 'success',
                'file': filepath
            })
            
        except Exception as e:
            logger.error(f"Error extracting data for {city['name']}: {str(e)}")
            results.append({
                'city': city['name'],
                'status': 'error',
                'error': str(e)
            })
    
    # Push results to XCom
    kwargs['ti'].xcom_push(key='extraction_results', value=results)
    return results

def transform_weather_data(**kwargs):
    """Transform and clean the weather data"""
    import pandas as pd
    import glob
    import os
    
    logger = logging.getLogger("airflow.task")
    logger.info("Starting data transformation")
    
    # Create directories if they don't exist
    os.makedirs("/tmp/weather_data/processed", exist_ok=True)
    
    try:
        # Load all JSON files
        file_pattern = "/tmp/weather_data/raw/*.json"
        files = glob.glob(file_pattern)
        
        if not files:
            logger.warning("No raw data files found")
            return {'status': 'no_data'}
        
        raw_data = []
        for file_path in files:
            with open(file_path, 'r') as file:
                data = json.load(file)
                raw_data.append(data)
        
        # Process into a dataframe (simplified version of the full transformation)
        records = []
        for record in raw_data:
            try:
                city_name = record['city_name']
                country_code = record['country_code']
                
                if 'dt' in record:
                    timestamp = datetime.fromtimestamp(record['dt'])
                else:
                    timestamp = datetime.fromisoformat(record['extraction_timestamp'])
                
                temperature = record['main'].get('temp', None)
                humidity = record['main'].get('humidity', None)
                pressure = record['main'].get('pressure', None)
                wind_speed = record['wind'].get('speed', None)
                weather_main = record['weather'][0].get('main', None) if record['weather'] else None
                
                clean_record = {
                    'city': city_name,
                    'country': country_code,
                    'timestamp': timestamp,
                    'temperature': temperature,
                    'humidity': humidity,
                    'pressure': pressure,
                    'wind_speed': wind_speed,
                    'weather_condition': weather_main
                }
                
                records.append(clean_record)
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
        
        # Create and save dataframe
        df = pd.DataFrame(records)
        output_file = f"/tmp/weather_data/processed/weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_file, index=False)
        
        logger.info(f"Transformed data saved to {output_file}")
        return {
            'status': 'success',
            'file': output_file,
            'record_count': len(df)
        }
    
    except Exception as e:
        logger.error(f"Error in transformation: {str(e)}")
        return {
            'status': 'error',
            'error': str(e)
        }

def analyze_weather_data(**kwargs):
    """Analyze the weather data and generate visualizations"""
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    import glob
    import os
    
    logger = logging.getLogger("airflow.task")
    logger.info("Starting data analysis")
    
    # Create directories if they don't exist
    os.makedirs("/tmp/weather_data/output", exist_ok=True)
    os.makedirs("/tmp/weather_data/output/figures", exist_ok=True)
    
    try:
        # Find the latest processed file
        file_pattern = "/tmp/weather_data/processed/*.csv"
        files = glob.glob(file_pattern)
        
        if not files:
            logger.warning("No processed data files found")
            return {'status': 'no_data'}
        
        latest_file = max(files, key=os.path.getmtime)
        df = pd.read_csv(latest_file)
        
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Generate a simple analysis
        analysis = {
            'record_count': len(df),
            'cities': df['city'].unique().tolist(),
            'avg_temperature': df['temperature'].mean(),
            'min_temperature': df['temperature'].min(),
            'max_temperature': df['temperature'].max(),
            'avg_humidity': df['humidity'].mean(),
            'avg_wind_speed': df['wind_speed'].mean(),
            'weather_conditions': df['weather_condition'].value_counts().to_dict()
        }
        
        # Save analysis results
        analysis_file = f"/tmp/weather_data/output/analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(analysis_file, 'w') as f:
            json.dump(analysis, f, indent=2)
        
        # Create a simple visualization - temperature comparison across cities
        plt.figure(figsize=(12, 6))
        city_temp = df.groupby('city')['temperature'].mean().sort_values(ascending=False)
        city_temp.plot(kind='bar', color='skyblue')
        plt.title('Average Temperature by City')
        plt.ylabel('Temperature (°C)')
        plt.xlabel('City')
        plt.tight_layout()
        
        fig_file = f"/tmp/weather_data/output/figures/temp_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        plt.savefig(fig_file)
        plt.close()
        
        logger.info(f"Analysis saved to {analysis_file}")
        logger.info(f"Figure saved to {fig_file}")
        
        return {
            'status': 'success',
            'analysis_file': analysis_file,
            'figures': [fig_file]
        }
        
    except Exception as e:
        logger.error(f"Error in analysis: {str(e)}")
        return {
            'status': 'error',
            'error': str(e)
        }

def load_weather_data(**kwargs):
    """Load the data to different formats"""
    import pandas as pd
    import glob
    import os
    import sqlite3
    
    logger = logging.getLogger("airflow.task")
    logger.info("Starting data loading")
    
    # Create directories if they don't exist
    os.makedirs("/tmp/weather_data/output", exist_ok=True)
    
    try:
        # Find the latest processed file
        file_pattern = "/tmp/weather_data/processed/*.csv"
        files = glob.glob(file_pattern)
        
        if not files:
            logger.warning("No processed data files found")
            return {'status': 'no_data'}
        
        latest_file = max(files, key=os.path.getmtime)
        df = pd.read_csv(latest_file)
        
        results = {}
        
        # Save to JSON
        json_file = f"/tmp/weather_data/output/weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        df.to_json(json_file, orient='records', indent=2)
        results['json_file'] = json_file
        
        # Save to SQLite
        db_file = f"/tmp/weather_data/output/weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        conn = sqlite3.connect(db_file)
        df.to_sql('weather_data', conn, if_exists='replace', index=False)
        conn.execute('CREATE INDEX IF NOT EXISTS idx_city ON weather_data (city)')
        conn.commit()
        conn.close()
        results['db_file'] = db_file
        
        # Save summary
        summary_file = f"/tmp/weather_data/output/weather_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.groupby('city').agg({
            'temperature': ['mean', 'min', 'max'],
            'humidity': ['mean'],
            'wind_speed': ['mean']
        }).to_csv(summary_file)
        results['summary_file'] = summary_file
        
        logger.info(f"Data loaded to JSON: {json_file}")
        logger.info(f"Data loaded to SQLite: {db_file}")
        logger.info(f"Summary saved to: {summary_file}")
        
        return {
            'status': 'success',
            'files': results
        }
        
    except Exception as e:
        logger.error(f"Error in loading: {str(e)}")
        return {
            'status': 'error',
            'error': str(e)
        }

def send_notification(**kwargs):
    """Send a notification when the pipeline is complete"""
    logger = logging.getLogger("airflow.task")
    ti = kwargs['ti']
    
    extract_results = ti.xcom_pull(task_ids='extract_weather_data')
    transform_results = ti.xcom_pull(task_ids='transform_weather_data')
    analyze_results = ti.xcom_pull(task_ids='analyze_weather_data')
    load_results = ti.xcom_pull(task_ids='load_weather_data')
    
    # In a real scenario, you would send an email or Slack message
    notification = {
        'timestamp': datetime.now().isoformat(),
        'status': 'success',
        'extract': extract_results,
        'transform': transform_results,
        'analyze': analyze_results,
        'load': load_results
    }
    
    logger.info(f"Pipeline completed successfully: {notification}")
    return notification

# Define tasks
extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_weather_data,
    provide_context=True,
    dag=dag,
)

analyze_task = PythonOperator(
    task_id='analyze_weather_data',
    python_callable=analyze_weather_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_weather_data,
    provide_context=True,
    dag=dag,
)

notification_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> analyze_task >> load_task >> notification_task
