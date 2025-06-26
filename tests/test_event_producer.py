"""Unit tests for the event producer module."""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.producer.event_producer import PurchasingEventProducer
from src.utils.config_loader import ConfigLoader


class TestPurchasingEventProducer:
    """Test cases for PurchasingEventProducer."""
    
    @pytest.fixture
    def mock_config_loader(self):
        """Create a mock configuration loader."""
        config_loader = Mock(spec=ConfigLoader)
        config_loader.get_kafka_config.return_value = {
            'bootstrap_servers': 'localhost:9092',
            'topic_name': 'test_purchasing_events'
        }
        config_loader.get_producer_config.return_value = {
            'acks': '1',
            'retries': 3,
            'batch_size': 100,
            'linger_ms': 1000,
            'event_interval_seconds': 0.1,
            'max_events': 10
        }
        return config_loader
    
    @pytest.fixture
    def mock_kafka_producer(self):
        """Create a mock Kafka producer."""
        producer = Mock()
        producer.send.return_value = Mock()
        producer.send.return_value.get.return_value = Mock(
            topic='test_purchasing_events',
            partition=0,
            offset=123
        )
        return producer
    
    @patch('src.producer.event_producer.setup_logging')
    @patch('src.producer.event_producer.get_logger')
    def test_producer_initialization(self, mock_get_logger, mock_setup_logging, mock_config_loader):
        """Test producer initialization."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        with patch('src.producer.event_producer.KafkaProducer'):
            producer = PurchasingEventProducer(mock_config_loader)
            
            assert producer.config_loader == mock_config_loader
            assert producer.kafka_config == mock_config_loader.get_kafka_config.return_value
            assert producer.producer_config == mock_config_loader.get_producer_config.return_value
    
    def test_generate_purchasing_event(self, mock_config_loader):
        """Test purchasing event generation."""
        with patch('src.producer.event_producer.KafkaProducer'):
            producer = PurchasingEventProducer(mock_config_loader)
            
            event = producer.generate_purchasing_event()
            
            # Check required fields
            required_fields = [
                'event_id', 'timestamp', 'customer_id', 'product_category',
                'quantity', 'unit_price', 'total_amount', 'currency'
            ]
            
            for field in required_fields:
                assert field in event
            
            # Check data types and constraints
            assert isinstance(event['quantity'], int)
            assert event['quantity'] > 0
            assert isinstance(event['unit_price'], float)
            assert event['unit_price'] > 0
            assert isinstance(event['total_amount'], float)
            assert event['total_amount'] > 0
            assert event['currency'] == 'USD'
            
            # Check calculated total
            base_amount = event['quantity'] * event['unit_price']
            discount_amount = base_amount * (event['discount_percentage'] / 100)
            expected_total = round(base_amount - discount_amount, 2)
            assert event['total_amount'] == expected_total
    
    def test_publish_event_success(self, mock_config_loader, mock_kafka_producer):
        """Test successful event publishing."""
        with patch('src.producer.event_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = PurchasingEventProducer(mock_config_loader)
            
            test_event = {
                'event_id': 'test_123',
                'customer_id': 'customer_456',
                'total_amount': 100.50
            }
            
            result = producer.publish_event(test_event)
            
            assert result is True
            mock_kafka_producer.send.assert_called_once()
    
    def test_publish_event_failure(self, mock_config_loader, mock_kafka_producer):
        """Test event publishing failure."""
        mock_kafka_producer.send.side_effect = Exception("Kafka connection failed")
        
        with patch('src.producer.event_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = PurchasingEventProducer(mock_config_loader)
            
            test_event = {
                'event_id': 'test_123',
                'customer_id': 'customer_456',
                'total_amount': 100.50
            }
            
            result = producer.publish_event(test_event)
            
            assert result is False
    
    def test_product_categories_coverage(self, mock_config_loader):
        """Test that all product categories are used."""
        with patch('src.producer.event_producer.KafkaProducer'):
            producer = PurchasingEventProducer(mock_config_loader)
            
            categories_found = set()
            
            # Generate multiple events to check category coverage
            for _ in range(100):
                event = producer.generate_purchasing_event()
                categories_found.add(event['product_category'])
            
            expected_categories = set(producer.product_categories.keys())
            
            # Should find most categories with enough iterations
            assert len(categories_found) >= len(expected_categories) * 0.7
    
    def test_event_id_uniqueness(self, mock_config_loader):
        """Test that generated event IDs are unique."""
        with patch('src.producer.event_producer.KafkaProducer'):
            producer = PurchasingEventProducer(mock_config_loader)
            
            event_ids = set()
            
            for _ in range(100):
                event = producer.generate_purchasing_event()
                event_id = event['event_id']
                assert event_id not in event_ids, f"Duplicate event ID: {event_id}"
                event_ids.add(event_id)
    
    def test_timestamp_format(self, mock_config_loader):
        """Test that timestamps are in correct ISO format."""
        with patch('src.producer.event_producer.KafkaProducer'):
            producer = PurchasingEventProducer(mock_config_loader)
            
            event = producer.generate_purchasing_event()
            timestamp_str = event['timestamp']
            
            # Should be able to parse as ISO format
            try:
                parsed_timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                assert parsed_timestamp is not None
            except ValueError:
                pytest.fail(f"Invalid timestamp format: {timestamp_str}")
    
    def test_discount_logic(self, mock_config_loader):
        """Test discount calculation logic."""
        with patch('src.producer.event_producer.KafkaProducer'):
            producer = PurchasingEventProducer(mock_config_loader)
            
            # Generate events and check discount application
            events_with_discount = 0
            total_events = 100
            
            for _ in range(total_events):
                event = producer.generate_purchasing_event()
                
                if event['discount_percentage'] > 0:
                    events_with_discount += 1
                    
                    # Verify discount calculation
                    base_amount = event['quantity'] * event['unit_price']
                    discount_amount = base_amount * (event['discount_percentage'] / 100)
                    expected_total = round(base_amount - discount_amount, 2)
                    assert event['total_amount'] == expected_total
            
            # Roughly 30% should have discounts based on random logic
            discount_ratio = events_with_discount / total_events
            assert 0.1 < discount_ratio < 0.5
    
    def test_close_method(self, mock_config_loader, mock_kafka_producer):
        """Test producer cleanup."""
        with patch('src.producer.event_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = PurchasingEventProducer(mock_config_loader)
            
            producer.close()
            
            mock_kafka_producer.flush.assert_called_once()
            mock_kafka_producer.close.assert_called_once()


class TestEventProducerIntegration:
    """Integration tests for event producer."""
    
    def test_config_loading(self):
        """Test configuration loading from file."""
        with patch('os.path.exists', return_value=True):
            with patch('builtins.open', create=True) as mock_open:
                mock_open.return_value.__enter__.return_value.read.return_value = """
                kafka:
                  bootstrap_servers: "test:9092"
                  topic_name: "test_topic"
                producer:
                  max_events: 100
                """
                
                config_loader = ConfigLoader()
                
                with patch('src.producer.event_producer.KafkaProducer'):
                    producer = PurchasingEventProducer(config_loader)
                    
                    assert producer.kafka_config['topic_name'] == 'test_topic' 