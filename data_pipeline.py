"""
Data Pipeline Script
A modular data pipeline with extraction, transformation, and loading stages.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize pipeline with configuration"""
        self.config = config
        self.data = []
        logger.info("Pipeline initialized")
    
    def extract(self, source_path: str) -> List[Dict[str, Any]]:
        """Extract data from CSV or JSON source"""
        try:
            path = Path(source_path)
            
            if path.suffix.lower() == '.csv':
                self.data = self._read_csv(source_path)
            elif path.suffix.lower() == '.json':
                self.data = self._read_json(source_path)
            else:
                raise ValueError(f"Unsupported file format: {path.suffix}")
            
            logger.info(f"Extracted {len(self.data)} records from {source_path}")
            return self.data
        
        except FileNotFoundError:
            logger.error(f"Source file not found: {source_path}")
            raise
        except Exception as e:
            logger.error(f"Error during extraction: {str(e)}")
            raise
    
    def _read_csv(self, path: str) -> List[Dict[str, Any]]:
        """Read CSV file"""
        data = []
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        return data
    
    def _read_json(self, path: str) -> List[Dict[str, Any]]:
        """Read JSON file"""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]
    
    def transform(self, transformers: List[callable]) -> List[Dict[str, Any]]:
        """Apply transformation functions to data"""
        try:
            for transformer in transformers:
                self.data = [transformer(record) for record in self.data]
                logger.info(f"Applied transformation: {transformer.__name__}")
            
            return self.data
        
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise
    
    def validate(self, rules: Dict[str, callable]) -> bool:
        """Validate data against rules"""
        try:
            invalid_count = 0
            
            for idx, record in enumerate(self.data):
                for field, validator in rules.items():
                    if not validator(record.get(field)):
                        logger.warning(f"Validation failed at record {idx}, field '{field}'")
                        invalid_count += 1
            
            logger.info(f"Validation complete: {invalid_count} issues found")
            return invalid_count == 0
        
        except Exception as e:
            logger.error(f"Error during validation: {str(e)}")
            raise
    
    def load(self, destination_path: str, format: str = 'json') -> None:
        """Load data to destination"""
        try:
            path = Path(destination_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            if format.lower() == 'json':
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(self.data, f, indent=2)
            
            elif format.lower() == 'csv':
                if self.data:
                    keys = self.data[0].keys()
                    with open(path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=keys)
                        writer.writeheader()
                        writer.writerows(self.data)
            
            else:
                raise ValueError(f"Unsupported output format: {format}")
            
            logger.info(f"Loaded {len(self.data)} records to {destination_path}")
        
        except Exception as e:
            logger.error(f"Error during load: {str(e)}")
            raise
    
    def get_summary(self) -> Dict[str, Any]:
        """Return pipeline summary"""
        return {
            'timestamp': datetime.now().isoformat(),
            'record_count': len(self.data),
            'sample': self.data[:2] if self.data else []
        }


# Example transformation functions
def clean_whitespace(record: Dict) -> Dict:
    """Remove leading/trailing whitespace from all string fields"""
    return {k: v.strip() if isinstance(v, str) else v for k, v in record.items()}


def uppercase_names(record: Dict) -> Dict:
    """Convert name field to uppercase"""
    if 'name' in record:
        record['name'] = record['name'].upper()
    return record


def add_timestamp(record: Dict) -> Dict:
    """Add processing timestamp"""
    record['processed_at'] = datetime.now().isoformat()
    return record


# Example validators
def validate_not_empty(value):
    """Check if value is not empty"""
    return value is not None and str(value).strip() != ""


def validate_email(value):
    """Basic email validation"""
    return "@" in str(value) if value else False


# Example usage
if __name__ == "__main__":
    # Configuration
    config = {
        'source': 'input_data.csv',
        'destination': 'output_data.json',
        'output_format': 'json'
    }
    
    try:
        # Initialize pipeline
        pipeline = DataPipeline(config)
        
        # Extract data
        pipeline.extract(config['source'])
        
        # Transform data
        transformers = [
            clean_whitespace,
            uppercase_names,
            add_timestamp
        ]
        pipeline.transform(transformers)
        
        # Validate data
        validators = {
            'name': validate_not_empty,
            'email': validate_email
        }
        is_valid = pipeline.validate(validators)
        
        # Load data
        pipeline.load(
            config['destination'],
            format=config['output_format']
        )
        
        # Print summary
        summary = pipeline.get_summary()
        logger.info(f"Pipeline Summary: {json.dumps(summary, indent=2)}")
        
        logger.info("Pipeline completed successfully!")
    
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        exit(1)
