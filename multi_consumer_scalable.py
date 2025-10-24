import json
import asyncio
import logging
import hashlib
import threading
import time
import os
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from db.async_postgres_optimized import OptimizedAsyncPostgres
from typing import List, Dict, Set
from datetime import datetime
import random

# Kafka configuration
KAFKA_BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"  # Use KRaft cluster
KAFKA_TOPIC = "sensor_readings"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ScalableKafkaConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, consumer_id: int, 
                 group_id: str = "scalable_etl_group", batch_size: int = 1000):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_id = consumer_id
        self.group_id = group_id
        self.batch_size = batch_size
        self.consumer = None
        self.db = None
        self.total_processed = 0
        self.start_time = None
        self.processed_hashes = set()
        self.duplicate_count = 0
        self.partition_stats = {}
        
    async def init(self):
        """Initialize consumer and database connection"""
        # Initialize database
        self.db = OptimizedAsyncPostgres()
        await self.db.init()
        
        # Initialize Kafka consumer with optimized settings
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            max_poll_records=1000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_interval_ms=300000,
            fetch_min_bytes=1024,
            fetch_max_wait_ms=500,
            consumer_timeout_ms=1000,
            client_id=f"consumer-{self.consumer_id}"  # Unique client ID
        )
        
        logger.info(f"Consumer {self.consumer_id} initialized for topic: {self.topic}")
        logger.info(f"Consumer {self.consumer_id}: Batch size: {self.batch_size}")
        logger.info(f"Consumer {self.consumer_id}: Consumer group: {self.group_id}")

    def _create_record_hash(self, record: Dict) -> str:
        """Create a hash for a record to detect duplicates"""
        record_str = f"{record['timestamp']}_{record['temperature']}_{record['pressure']}"
        return hashlib.md5(record_str.encode()).hexdigest()

    def _is_duplicate(self, record: Dict) -> bool:
        """Check if record is a duplicate"""
        record_hash = self._create_record_hash(record)
        if record_hash in self.processed_hashes:
            return True
        self.processed_hashes.add(record_hash)
        return False

    async def check_database_duplicates(self, records: List[Dict]) -> List[Dict]:
        """Check for duplicates in database before insertion"""
        if not records:
            return records
            
        try:
            async with self.db.pool.acquire() as conn:
                # Check for existing records using individual comparisons
                existing_records = []
                for record in records:
                    # Convert timestamp string to datetime for proper comparison
                    timestamp_dt = datetime.fromisoformat(record["timestamp"])
                    
                    existing = await conn.fetchrow("""
                        SELECT timestamp, temperature, humidity, pressure, location 
                        FROM sensors 
                        WHERE timestamp = $1 AND temperature = $2 AND humidity = $3 AND pressure = $4 AND location = $5
                        LIMIT 1
                    """, timestamp_dt, record["temperature"], record["humidity"], record["pressure"], record["location"])
                    if existing:
                        existing_records.append(existing)
                
                # Create set of existing records
                existing_set = set()
                for row in existing_records:
                    existing_set.add((row["timestamp"], row["temperature"], row["humidity"], row["pressure"], row["location"]))
                
                # Filter out duplicates
                unique_records = []
                for record in records:
                    # Convert timestamp for consistent comparison
                    timestamp_dt = datetime.fromisoformat(record["timestamp"])
                    record_key = (timestamp_dt, record["temperature"], record["humidity"], record["pressure"], record["location"])
                    if record_key not in existing_set:
                        unique_records.append(record)
                    else:
                        self.duplicate_count += 1
                
                if self.duplicate_count > 0:
                    logger.warning(f"Consumer {self.consumer_id}: Found {self.duplicate_count} duplicate records in database")
                
                return unique_records
                
        except Exception as e:
            logger.error(f"Consumer {self.consumer_id}: Error checking database duplicates: {e}")
            return records  # Return all records if check fails

    async def process_batch(self, records: List[Dict]) -> None:
        """Process a batch of records with duplicate detection"""
        if not records:
            return
            
        try:
            # Filter out in-memory duplicates
            unique_records = []
            for record in records:
                if not self._is_duplicate(record):
                    unique_records.append(record)
            
            # Check for database duplicates
            final_records = await self.check_database_duplicates(unique_records)
            
            if not final_records:
                logger.info(f"Consumer {self.consumer_id}: No new records to insert (all duplicates)")
                return
            
            # Insert batch to database
            await self.db.insert_batch(final_records)
            
            self.total_processed += len(final_records)
            logger.info(f"Consumer {self.consumer_id}: Processed batch of {len(final_records)} unique records. Total: {self.total_processed}")
            
            # Log performance metrics
            if self.start_time:
                elapsed = time.time() - self.start_time
                rate = self.total_processed / elapsed if elapsed > 0 else 0
                logger.info(f"Consumer {self.consumer_id}: Processing rate: {rate:.2f} records/second")
                
        except Exception as e:
            logger.error(f"Consumer {self.consumer_id}: Error processing batch: {e}")
            raise

    async def consume_messages(self):
        """Main consumption loop"""
        self.start_time = time.time()
        self.buffer = []  # Initialize buffer
        logger.info(f"Consumer {self.consumer_id}: Starting message consumption...")
        
        try:
            for message in self.consumer:
                # Track partition statistics
                partition = message.partition
                if partition not in self.partition_stats:
                    self.partition_stats[partition] = 0
                self.partition_stats[partition] += 1
                
                # Add message to buffer
                # Debug: Print first message structure
                if len(self.buffer) == 0:
                    logger.info(f"Consumer {self.consumer_id}: First message structure: {type(message.value)} - {message.value}")
                self.buffer.append(message.value)
                
                # Process batch when buffer is full
                if len(self.buffer) >= self.batch_size:
                    await self.process_batch(self.buffer.copy())
                    self.buffer.clear()
                    
        except KeyboardInterrupt:
            logger.info(f"Consumer {self.consumer_id}: Received interrupt signal, shutting down gracefully...")
        except KafkaError as e:
            logger.error(f"Consumer {self.consumer_id}: Kafka error: {e}")
            raise
        except Exception as e:
            logger.error(f"Consumer {self.consumer_id}: Unexpected error: {e}")
            raise
        finally:
            # Process remaining records in buffer
            if hasattr(self, 'buffer') and self.buffer:
                logger.info(f"Consumer {self.consumer_id}: Processing remaining {len(self.buffer)} records...")
                await self.process_batch(self.buffer)
                self.buffer.clear()
            
            # Close consumer
            if self.consumer:
                self.consumer.close()
                logger.info(f"Consumer {self.consumer_id}: Closed")
            
            # Log final statistics
            if self.start_time:
                total_time = time.time() - self.start_time
                final_rate = self.total_processed / total_time if total_time > 0 else 0
                logger.info(f"Consumer {self.consumer_id} Final statistics:")
                logger.info(f"Total records processed: {self.total_processed}")
                logger.info(f"Duplicate records filtered: {self.duplicate_count}")
                logger.info(f"Total time: {total_time:.2f} seconds")
                logger.info(f"Average rate: {final_rate:.2f} records/second")
                logger.info(f"Partition distribution: {self.partition_stats}")

    async def close(self):
        """Close consumer and database connections"""
        if self.consumer:
            self.consumer.close()
        if self.db:
            await self.db.close()

async def run_consumer(consumer_id: int, bootstrap_servers: str, topic: str, group_id: str):
    """Run a single consumer instance"""
    consumer = ScalableKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        consumer_id=consumer_id,
        group_id=group_id,
        batch_size=1000
    )
    
    try:
        await consumer.init()
        await consumer.consume_messages()
    except Exception as e:
        logger.error(f"Consumer {consumer_id} failed: {e}")
        raise
    finally:
        await consumer.close()

async def main():
    """Main execution function with multiple consumers"""
    start_time = time.time()
    
    try:
        # Configuration
        bootstrap_servers = KAFKA_BROKERS
        topic = KAFKA_TOPIC
        group_id = "scalable_etl_group"
        consumer_count = int(os.environ.get('CONSUMER_COUNT', '3'))  # Configurable via environment
        
        logger.info(f"Starting {consumer_count} consumers in consumer group: {group_id}")
        
        # Run consumers concurrently
        tasks = []
        for i in range(consumer_count):
            task = run_consumer(i+1, bootstrap_servers, topic, group_id)
            tasks.append(task)
        
        # Wait for all consumers to complete
        await asyncio.gather(*tasks)
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"Multi-consumer pipeline completed successfully!")
        logger.info(f"Total time: {duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Multi-consumer pipeline failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
