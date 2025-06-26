"""Event producer for generating and publishing purchasing events to Kafka."""

import json
import time
import random
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.utils.config_loader import ConfigLoader
from src.utils.logger import setup_logging, get_logger


class PurchasingEventProducer:
    """Producer for generating realistic purchasing events and publishing to Kafka."""
    
    def __init__(self, config_loader: Optional[ConfigLoader] = None):
        """Initialize the purchasing event producer.
        
        Args:
            config_loader (ConfigLoader, optional): Configuration loader instance
        """
        self.config_loader = config_loader or ConfigLoader()
        self.logger = get_logger(__name__)
        
        # Load configuration
        self.kafka_config = self.config_loader.get_kafka_config()
        self.producer_config = self.config_loader.get_producer_config()
        
        # Initialize Kafka producer
        self.producer = self._create_producer()
        
        # Product categories and price ranges for realistic data generation
        self.product_categories = {
            'Electronics': {'min_price': 50, 'max_price': 2000},
            'Clothing': {'min_price': 20, 'max_price': 300},
            'Books': {'min_price': 10, 'max_price': 80},
            'Home': {'min_price': 25, 'max_price': 500},
            'Sports': {'min_price': 15, 'max_price': 400},
            'Food': {'min_price': 5, 'max_price': 100}
        }
        
        self.customer_segments = ['Premium', 'Regular', 'Budget']
        self.payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']
        
    def _create_producer(self) -> KafkaProducer:
        """Create and configure Kafka producer.
        
        Returns:
            KafkaProducer: Configured Kafka producer instance
            
        Raises:
            KafkaError: If producer creation fails
        """
        try:
            producer_settings = {
                'bootstrap_servers': self.kafka_config.get('bootstrap_servers', 'localhost:9092'),
                'value_serializer': lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                'key_serializer': lambda k: str(k).encode('utf-8') if k else None,
                'acks': int(self.producer_config.get('acks', 1)),  # Ensure integer
                'retries': int(self.producer_config.get('retries', 3)),  # Ensure integer  
                'batch_size': int(self.producer_config.get('batch_size', 100)),  # Ensure integer
                'linger_ms': int(self.producer_config.get('linger_ms', 1000)),  # Ensure integer
            }
            
            self.logger.info("Creating Kafka producer", settings=producer_settings)
            return KafkaProducer(**producer_settings)
            
        except Exception as e:
            self.logger.error("Failed to create Kafka producer", error=str(e))
            raise KafkaError(f"Producer creation failed: {e}")
    
    def generate_purchasing_event(self) -> Dict[str, Any]:
        """Generate a realistic purchasing event.
        
        Returns:
            Dict[str, Any]: Purchasing event data
        """
        # Select random product category
        category = random.choice(list(self.product_categories.keys()))
        price_range = self.product_categories[category]
        
        # Generate event data
        event = {
            'event_id': f"purchase_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'customer_id': f"customer_{random.randint(1, 10000)}",
            'customer_segment': random.choice(self.customer_segments),
            'product_id': f"product_{category.lower()}_{random.randint(1, 1000)}",
            'product_category': category,
            'product_name': f"{category} Item {random.randint(1, 100)}",
            'quantity': random.randint(1, 5),
            'unit_price': round(random.uniform(price_range['min_price'], price_range['max_price']), 2),
            'total_amount': 0,  # Will be calculated
            'currency': 'USD',
            'payment_method': random.choice(self.payment_methods),
            'order_source': random.choice(['Web', 'Mobile App', 'Store']),
            'location': {
                'country': random.choice(['US', 'CA', 'UK', 'DE', 'FR']),
                'city': random.choice(['New York', 'Los Angeles', 'Toronto', 'London', 'Berlin', 'Paris'])
            },
            'discount_percentage': round(random.uniform(0, 25), 2) if random.random() < 0.3 else 0,
            'is_new_customer': random.choice([True, False]),
            'device_type': random.choice(['Desktop', 'Mobile', 'Tablet'])
        }
        
        # Calculate total amount with discount
        base_amount = event['quantity'] * event['unit_price']
        discount_amount = base_amount * (event['discount_percentage'] / 100)
        event['total_amount'] = round(base_amount - discount_amount, 2)
        
        return event
    
    def publish_event(self, event: Dict[str, Any]) -> bool:
        """Publish a single event to Kafka.
        
        Args:
            event (Dict[str, Any]): Event data to publish
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            topic = self.kafka_config.get('topic_name', 'purchasing_events')
            key = event.get('customer_id')
            
            # Send to Kafka
            future = self.producer.send(topic, key=key, value=event)
            record_metadata = future.get(timeout=10)
            
            self.logger.info(
                "Event published successfully",
                topic=record_metadata.topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                event_id=event.get('event_id')
            )
            
            return True
            
        except Exception as e:
            self.logger.error(
                "Failed to publish event",
                error=str(e),
                event_id=event.get('event_id')
            )
            return False
    
    def start_streaming(self) -> None:
        """Start streaming purchasing events to Kafka."""
        max_events = self.producer_config.get('max_events', 1000)
        interval_seconds = self.producer_config.get('event_interval_seconds', 1)
        
        self.logger.info(
            "Starting event streaming",
            max_events=max_events,
            interval_seconds=interval_seconds
        )
        
        events_published = 0
        
        try:
            while events_published < max_events:
                # Generate and publish event
                event = self.generate_purchasing_event()
                
                if self.publish_event(event):
                    events_published += 1
                    
                    if events_published % 100 == 0:
                        self.logger.info(f"Published {events_published} events")
                
                # Wait before next event
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            self.logger.info("Streaming interrupted by user")
        except Exception as e:
            self.logger.error("Streaming failed", error=str(e))
        finally:
            self.close()
            
        self.logger.info(f"Streaming completed. Total events published: {events_published}")
    
    def close(self) -> None:
        """Close the Kafka producer and cleanup resources."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.logger.info("Producer closed successfully")


def main():
    """Main function to run the event producer."""
    # Setup logging
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting Purchasing Event Producer")
        
        # Create and start producer
        producer = PurchasingEventProducer()
        producer.start_streaming()
        
    except Exception as e:
        logger.error("Producer failed", error=str(e))
        raise


if __name__ == "__main__":
    main() 