"""Configuration loader utility for the streaming data pipeline."""

import os
import yaml
from typing import Dict, Any


class ConfigLoader:
    """Load and manage application configuration from YAML file."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the configuration loader.
        
        Args:
            config_path (str): Path to the configuration file
        """
        self.config_path = config_path
        self._config = None
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file.
        
        Returns:
            Dict[str, Any]: Configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is invalid YAML
        """
        if self._config is None:
            if not os.path.exists(self.config_path):
                raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
                
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self._config = yaml.safe_load(file)
                
        return self._config
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka configuration section.
        
        Returns:
            Dict[str, Any]: Kafka configuration
        """
        config = self.load_config()
        return config.get('kafka', {})
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get producer configuration section.
        
        Returns:
            Dict[str, Any]: Producer configuration
        """
        config = self.load_config()
        return config.get('producer', {})
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration section.
        
        Returns:
            Dict[str, Any]: Spark configuration
        """
        config = self.load_config()
        return config.get('spark', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration section.
        
        Returns:
            Dict[str, Any]: Logging configuration
        """
        config = self.load_config()
        return config.get('logging', {}) 