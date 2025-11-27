#!/usr/bin/env python3
"""
Weather Data Pipeline - Main execution script.

Workflow:
1. Parse command line arguments for custom configurations and execution options
2. Set up logging based on the configuration
3. Initialize monitoring for real-time metrics
4. Execute the pipeline phases: extract, transform, analyze (load)
5. Handle errors and provide appropriate status reporting
6. Support selective execution of pipeline phases for flexibility
7. Orchestrate the complete data pipeline end-to-end
"""
import os
import logging
import argparse
import yaml
import time
from datetime import datetime
from src.extract import WeatherDataExtractor
from src.transform import WeatherDataTransformer
from src.analyze import WeatherDataAnalyzer
from src.load import WeatherDataLoader
from src.utils import setup_monitoring
from prometheus_client import Counter, Summary, start_http_server 

# Prometheus metrics
PIPELINE_RUNS = Counter('weather_pipeline_runs', 'Number of pipeline runs')
PIPELINE_FAILURES = Counter('weather_pipeline_failures', 'Number of pipeline failures')
PIPELINE_DURATION = Summary('weather_pipeline_duration_seconds', 'Total pipeline execution time')

def setup_logging(config):
    """Set up logging based on configuration."""
    log_dir = os.path.dirname(config['logging']['file'])
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, config['logging']['level']),
        format=config['logging']['format'],
        handlers=[
            logging.FileHandler(config['logging']['file']),
            logging.StreamHandler()  # Also log to console
        ]
    )
    
    return logging.getLogger(__name__)

def load_config(config_path):
    """Load configuration from YAML file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

@PIPELINE_DURATION.time()
def run_pipeline(config_path, skip_extract=False, skip_transform=False, skip_analyze=False, skip_load=False):
    """
    Run the complete weather data pipeline.
    
    Args:
        config_path: Path to the configuration file.
        skip_extract: Whether to skip the extraction step.
        skip_transform: Whether to skip the transformation step.
        skip_analyze: Whether to skip the analysis step.
        skip_load: Whether to skip the loading step.
    """
    # Load config
    config = load_config(config_path)
    
    # Setup logging
    logger = setup_logging(config)
    
    # Setup monitoring
    setup_monitoring(config)
    
    logger.info("Starting Weather Data Pipeline")
    start_time = time.time()
    
    PIPELINE_RUNS.inc()
    success = True
    
    try:
        # Create required directories
        os.makedirs(config['data']['raw_data_path'], exist_ok=True)
        os.makedirs(config['data']['processed_data_path'], exist_ok=True)
        os.makedirs(config['data']['output_data_path'], exist_ok=True)
        
        # Extract data
        if not skip_extract:
            logger.info("Starting extraction phase")
            extractor = WeatherDataExtractor(config_path)
            extracted_data = extractor.extract_current_weather()
            logger.info(f"Extraction complete. Extracted data for {len(extracted_data)} cities")
        else:
            logger.info("Skipping extraction phase")
        
        # Transform data
        if not skip_transform:
            logger.info("Starting transformation phase")
            transformer = WeatherDataTransformer(config_path)
            transformed_data = transformer.transform_data()
            logger.info(f"Transformation complete. Transformed data shape: {transformed_data.shape}")
        else:
            logger.info("Skipping transformation phase")
        
        # Analyze data
        if not skip_analyze:
            logger.info("Starting analysis phase")
            analyzer = WeatherDataAnalyzer(config_path)
            analysis_results = analyzer.analyze_data()
            logger.info("Analysis and visualization complete")
        else:
            logger.info("Skipping analysis phase")
            
        # Load data
        if not skip_load:
            logger.info("Starting loading phase")
            loader = WeatherDataLoader(config_path)
            json_result = loader.load_to_json()
            sqlite_result = loader.load_to_sqlite()
            csv_result = loader.load_to_csv()
            logger.info("Loading complete")
        else:
            logger.info("Skipping loading phase")
            
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        PIPELINE_FAILURES.inc()
        success = False
    
    # Log execution time
    execution_time = time.time() - start_time
    logger.info(f"Pipeline completed in {execution_time:.2f} seconds with status: {'Success' if success else 'Failure'}")
    
    return success

def main():
    """Parse command line arguments and run the pipeline."""
    parser = argparse.ArgumentParser(description='Weather Data Pipeline')
    parser.add_argument('--config', type=str, default='./config/config.yaml',
                        help='Path to the configuration file')
    parser.add_argument('--skip-extract', action='store_true',
                       help='Skip the extraction phase')
    parser.add_argument('--skip-transform', action='store_true',
                       help='Skip the transformation phase')
    parser.add_argument('--skip-analyze', action='store_true',
                       help='Skip the analysis phase')
    parser.add_argument('--skip-load', action='store_true',
                       help='Skip the loading phase')
    
    args = parser.parse_args()
    
    run_pipeline(
        args.config,
        skip_extract=args.skip_extract,
        skip_transform=args.skip_transform,
        skip_analyze=args.skip_analyze,
        skip_load=args.skip_load
    )

if __name__ == "__main__":
    main()

    # Keep the Prometheus metrics server running
    logging.info("Pipeline completed successfully. Keeping the Prometheus metrics server alive.")

    try:
        while True:
            time.sleep(300)  # Keep running indefinitely
    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")