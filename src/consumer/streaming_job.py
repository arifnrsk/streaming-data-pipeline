"""PySpark streaming job for processing purchasing events from Kafka."""

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, sum as spark_sum, 
    current_timestamp, window, date_format, 
    count, avg, max as spark_max, min as spark_min, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, IntegerType, BooleanType, TimestampType
)
from typing import Optional
import sys
import os
import platform
import tempfile

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.utils.config_loader import ConfigLoader
from src.utils.logger import setup_logging, get_logger


class StreamingProcessor:
    """PySpark streaming processor for purchasing events."""
    
    def __init__(self, config_loader: Optional[ConfigLoader] = None):
        """Initialize the streaming processor.
        
        Args:
            config_loader (ConfigLoader, optional): Configuration loader instance
        """
        self.config_loader = config_loader or ConfigLoader()
        self.logger = get_logger(__name__)
        
        # Load configuration
        self.kafka_config = self.config_loader.get_kafka_config()
        self.spark_config = self.config_loader.get_spark_config()
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Define schema for incoming events
        self.event_schema = self._define_event_schema()
        
        # Running totals for display
        self.running_total = 0.0
        
    def _get_platform_paths(self):
        """Get platform-specific paths for checkpoints and warehouse.
        
        Returns:
            tuple: (checkpoint_path, warehouse_path)
        """
        system = platform.system().lower()
        
        if system == 'windows':
            # Windows paths
            checkpoint_path = "C:/tmp/spark-checkpoints"
            warehouse_path = "C:/tmp/spark-warehouse"
            
            # Ensure directories exist
            os.makedirs(checkpoint_path, exist_ok=True)
            os.makedirs(warehouse_path, exist_ok=True)
            
        else:
            # Linux/Unix paths - use temporary directory
            temp_dir = tempfile.gettempdir()
            checkpoint_path = os.path.join(temp_dir, "spark-checkpoints")
            warehouse_path = os.path.join(temp_dir, "spark-warehouse")
            
            # Ensure directories exist
            os.makedirs(checkpoint_path, exist_ok=True)
            os.makedirs(warehouse_path, exist_ok=True)
            
        return checkpoint_path, warehouse_path
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session.
        
        Returns:
            SparkSession: Configured Spark session
        """
        app_name = self.spark_config.get('app_name', 'StreamingDataPipeline')
        master = self.spark_config.get('master', 'local[*]')
        
        self.logger.info(f"Creating Spark session: {app_name}")
        
        # Get platform-specific paths
        checkpoint_path, warehouse_path = self._get_platform_paths()
        
        self.logger.info(f"Platform: {platform.system()}")
        self.logger.info(f"Checkpoint path: {checkpoint_path}")
        self.logger.info(f"Warehouse path: {warehouse_path}")
        
        # Minimal Spark configuration for maximum compatibility
        spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[1]") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.maxResultSize", "512m") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "localhost") \
            .config("spark.ui.enabled", "false") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    def _define_event_schema(self) -> StructType:
        """Define the schema for purchasing events.
        
        Returns:
            StructType: Event schema structure
        """
        location_schema = StructType([
            StructField("country", StringType(), True),
            StructField("city", StringType(), True)
        ])
        
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("order_source", StringType(), True),
            StructField("location", location_schema, True),
            StructField("discount_percentage", DoubleType(), True),
            StructField("is_new_customer", BooleanType(), True),
            StructField("device_type", StringType(), True)
        ])
    
    def create_kafka_stream(self):
        """Create streaming DataFrame from Kafka topic.
        
        Returns:
            DataFrame: Streaming DataFrame from Kafka
        """
        kafka_servers = self.kafka_config.get('bootstrap_servers', 'localhost:9092')
        topic = self.kafka_config.get('topic_name', 'purchasing_events')
        
        self.logger.info(f"Creating Kafka stream from topic: {topic}")
        
        # Read from Kafka
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data
        events_df = kafka_df.select(
            from_json(col("value").cast("string"), self.event_schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp to proper format
        events_df = events_df.withColumn(
            "event_timestamp",
            col("timestamp").cast(TimestampType())
        ).withColumn(
            "event_date",
            to_date(col("event_timestamp"))
        )
        
        return events_df
    
    def process_daily_aggregations(self, events_df):
        """Process daily purchase aggregations.
        
        Args:
            events_df: Input events DataFrame
            
        Returns:
            DataFrame: Aggregated results
        """
        # Daily aggregations
        daily_aggregations = events_df.groupBy("event_date") \
            .agg(
                spark_sum("total_amount").alias("daily_total"),
                count("event_id").alias("transaction_count"),
                avg("total_amount").alias("avg_transaction_amount"),
                spark_max("total_amount").alias("max_transaction_amount"),
                spark_min("total_amount").alias("min_transaction_amount")
            ) \
            .withColumn("timestamp", current_timestamp()) \
            .select(
                col("timestamp"),
                col("event_date"),
                col("daily_total"),
                col("transaction_count"),
                col("avg_transaction_amount"),
                col("max_transaction_amount"),
                col("min_transaction_amount")
            )
        
        return daily_aggregations
    
    def process_running_totals(self, events_df):
        """Process running totals for real-time display.
        
        Args:
            events_df: Input events DataFrame
            
        Returns:
            DataFrame: Running totals
        """
        # Window-based aggregation for running totals
        windowed_totals = events_df \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window(col("event_timestamp"), "1 minute")
            ) \
            .agg(
                spark_sum("total_amount").alias("window_total"),
                count("event_id").alias("window_count")
            ) \
            .withColumn("timestamp", current_timestamp()) \
            .select(
                col("timestamp"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("window_total"),
                col("window_count")
            )
        
        return windowed_totals
    
    def process_batch_with_aggregation(self, batch_df, batch_id):
        """Process each batch with daily aggregation logic (Assignment requirement).
        
        Args:
            batch_df: Batch DataFrame
            batch_id: Batch identifier
        """
        if batch_df.count() > 0:
            # Calculate batch totals
            batch_total = batch_df.agg(spark_sum("total_amount")).collect()[0][0] or 0.0
            batch_count = batch_df.count()
            
            # Update running total
            self.running_total += batch_total
            
            # Calculate daily aggregation as required by assignment
            daily_summary = batch_df.groupBy("event_date") \
                .agg(
                    spark_sum("total_amount").alias("daily_total"),
                    count("event_id").alias("transaction_count")
                ) \
                .withColumn("timestamp", current_timestamp()) \
                .withColumn("running_total", lit(self.running_total))
            
            # Display results in required format
            print(f"\n{'='*80}")
            print(f"STREAMING DATA PIPELINE OUTPUT - BATCH {batch_id}")
            print(f"{'='*80}")
            
            # Show daily aggregation (Assignment requirement)
            print("\nDAILY AGGREGATION SUMMARY:")
            daily_summary.select(
                date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("Timestamp"),
                col("event_date").alias("Date"),
                col("daily_total").alias("Daily_Total"),
                col("running_total").alias("Running_Total"),
                col("transaction_count").alias("Transaction_Count")
            ).show(truncate=False)
            
            # Show detailed batch data
            print("\nBATCH DETAILS:")
            batch_df.select(
                "event_id",
                "customer_id", 
                "product_category",
                "total_amount",
                date_format("event_timestamp", "yyyy-MM-dd HH:mm:ss").alias("event_time")
            ).show(10, truncate=False)
            
            # Show category breakdown
            print("\nCATEGORY BREAKDOWN:")
            batch_df.groupBy("product_category") \
                .agg(
                    spark_sum("total_amount").alias("category_total"),
                    count("event_id").alias("transaction_count")
                ) \
                .orderBy(col("category_total").desc()) \
                .show()
            
            print(f"{'='*80}")
            print(f"RUNNING TOTAL: ${self.running_total:,.2f}")
            print(f"{'='*80}\n")
    
    def start_streaming(self):
        """Start the streaming job."""
        self.logger.info("Starting streaming job...")
        
        try:
            # Create stream
            events_stream = self.create_kafka_stream()
            
            # Use foreachBatch with memory-only processing to avoid checkpointing issues
            # This processes each batch without requiring persistent checkpointing
            query = events_stream.writeStream \
                .foreachBatch(self.process_batch_with_aggregation) \
                .outputMode("append") \
                .trigger(processingTime=self.spark_config.get('trigger_processing_time', '10 seconds')) \
                .start()
            
            self.logger.info("Streaming query started successfully")
            print("\n" + "="*80)
            print("STREAMING DATA PIPELINE - STARTED")
            print("="*80)
            print("Listening for purchasing events...")
            print("Press Ctrl+C to stop")
            print("="*80 + "\n")
            
            # Wait for termination
            query.awaitTermination()
            
        except Exception as e:
            self.logger.error(f"Streaming job failed: {str(e)}")
            raise
        finally:
            self.stop()
    
    def stop(self):
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")


def main():
    """Main function to run the streaming processor."""
    # Setup logging
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting PySpark Streaming Job")
        
        # Create and start processor
        processor = StreamingProcessor()
        processor.start_streaming()
        
    except KeyboardInterrupt:
        logger.info("Streaming job interrupted by user")
    except Exception as e:
        logger.error(f"Streaming job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main() 