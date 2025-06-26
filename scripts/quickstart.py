#!/usr/bin/env python3
"""Quick start script to validate and demonstrate the streaming data pipeline."""

import sys
import os
import time
import subprocess
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config_loader import ConfigLoader
from src.utils.logger import setup_logging, get_logger


def check_requirements():
    """Check if all requirements are installed."""
    logger = get_logger(__name__)
    logger.info("Checking system requirements...")
    
    # Check Python version
    if sys.version_info < (3, 8):
        logger.error("Python 3.8+ is required")
        return False
    
    # Check required packages
    required_packages = ['pyspark', 'kafka', 'yaml', 'structlog']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        logger.error(f"Missing packages: {missing_packages}")
        logger.info("Run: pip install -r requirements.txt")
        return False
    
    logger.info("All requirements satisfied")
    return True


def check_docker():
    """Check if Docker is running."""
    logger = get_logger(__name__)
    logger.info("Checking Docker availability...")
    
    try:
        result = subprocess.run(['docker', 'ps'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            logger.info("Docker is running")
            return True
        else:
            logger.warning("Docker is not running or not accessible")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        logger.warning("Docker command not found or timed out")
        return False


def check_kafka_services():
    """Check if Kafka services are running."""
    logger = get_logger(__name__)
    logger.info("Checking Kafka services...")
    
    try:
        result = subprocess.run(['docker', 'ps', '--filter', 'name=streaming-kafka'], 
                              capture_output=True, text=True, timeout=10)
        
        if 'streaming-kafka' in result.stdout:
            logger.info("Kafka services are running")
            return True
        else:
            logger.warning("Kafka services not found")
            logger.info("Start with: make start-kafka")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        logger.warning("Could not check Kafka services")
        return False


def test_configuration():
    """Test configuration loading."""
    logger = get_logger(__name__)
    logger.info("Testing configuration...")
    
    try:
        config_loader = ConfigLoader()
        kafka_config = config_loader.get_kafka_config()
        producer_config = config_loader.get_producer_config()
        spark_config = config_loader.get_spark_config()
        
        logger.info(f"Kafka topic: {kafka_config.get('topic_name')}")
        logger.info(f"Max events: {producer_config.get('max_events')}")
        logger.info(f"Spark app: {spark_config.get('app_name')}")
        
        return True
    except Exception as e:
        logger.error(f"Configuration test failed: {e}")
        return False


def test_event_generation():
    """Test event generation without Kafka."""
    logger = get_logger(__name__)
    logger.info("Testing event generation...")
    
    try:
        from unittest.mock import patch
        from src.producer.event_producer import PurchasingEventProducer
        
        with patch('src.producer.event_producer.KafkaProducer'):
            producer = PurchasingEventProducer()
            event = producer.generate_purchasing_event()
            
            required_fields = ['event_id', 'timestamp', 'total_amount']
            for field in required_fields:
                if field not in event:
                    logger.error(f"Missing field in event: {field}")
                    return False
            
            logger.info(f"Generated event: {event['event_id']}")
            logger.info(f"Amount: ${event['total_amount']:.2f}")
            logger.info(f"Category: {event['product_category']}")
            
            return True
    except Exception as e:
        logger.error(f"Event generation test failed: {e}")
        return False


def run_health_check():
    """Run comprehensive health check."""
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info("STREAMING DATA PIPELINE - HEALTH CHECK")
    logger.info("=" * 60)
    
    checks = [
        ("Requirements", check_requirements),
        ("Docker", check_docker),
        ("Kafka Services", check_kafka_services),
        ("Configuration", test_configuration),
        ("Event Generation", test_event_generation),
    ]
    
    results = {}
    
    for check_name, check_func in checks:
        logger.info(f"\nRunning {check_name} check...")
        try:
            results[check_name] = check_func()
        except Exception as e:
            logger.error(f"{check_name} check failed with exception: {e}")
            results[check_name] = False
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("HEALTH CHECK SUMMARY")
    logger.info("=" * 60)
    
    all_passed = True
    for check_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        logger.info(f"{check_name:20}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        logger.info("\n✅ All checks passed! Ready to run the pipeline.")
        show_usage_instructions()
    else:
        logger.info("\n❌ Some checks failed. Please fix issues before proceeding.")
        show_troubleshooting_tips()
    
    return all_passed


def show_usage_instructions():
    """Show usage instructions."""
    logger = get_logger(__name__)
    logger.info("\n" + "=" * 60)
    logger.info("USAGE INSTRUCTIONS")
    logger.info("=" * 60)
    logger.info("1. Start Kafka (if not running): make start-kafka")
    logger.info("2. In Terminal 1: make run-consumer")
    logger.info("3. In Terminal 2: make run-producer")
    logger.info("4. Watch the real-time output!")
    logger.info("\nFor more commands: make help")


def show_troubleshooting_tips():
    """Show troubleshooting tips."""
    logger = get_logger(__name__)
    logger.info("\n" + "=" * 60)
    logger.info("TROUBLESHOOTING TIPS")
    logger.info("=" * 60)
    logger.info("• Missing packages: make install")
    logger.info("• Docker not running: Start Docker Desktop")
    logger.info("• Kafka not running: make start-kafka")
    logger.info("• Config issues: Check config/config.yaml")
    logger.info("• Port conflicts: make stop-kafka then restart")


def main():
    """Main function."""
    setup_logging()
    
    print("🚀 Streaming Data Pipeline - Quick Start")
    print("=" * 60)
    
    try:
        success = run_health_check()
        exit_code = 0 if success else 1
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nQuick start interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Quick start failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 