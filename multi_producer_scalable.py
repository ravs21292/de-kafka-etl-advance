import pandas as pd
import json
import asyncio
import time
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from typing import List, Dict
import logging
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ScalableKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str, producer_id: int, 
                 partition_count: int = 3, batch_size: int = 1000):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer_id = producer_id
        self.partition_count = partition_count
        self.batch_size = batch_size
        self.producer = None
        self.total_records = 0
        self.processed_records = 0
        self.data_hash = None
        self.record_hashes = set()
        self.partition_stats = {i: 0 for i in range(partition_count)}
        
    def connect(self, max_retries: int = 10):
        """Connect to Kafka with retry logic"""
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    batch_size=8192,   # 8KB batch size (smaller for faster processing)
                    linger_ms=5,       # Wait up to 5ms to batch messages
                    compression_type='gzip',  # Compress messages
                    acks='all',        # Required for idempotent producer
                    retries=3,         # Retry failed sends
                    retry_backoff_ms=100,
                    max_in_flight_requests_per_connection=1,
                    enable_idempotence=True,  # Prevent duplicate messages
                    request_timeout_ms=10000,  # 10 second timeout
                    delivery_timeout_ms=30000  # 30 second delivery timeout
                )
                logger.info(f"Producer {self.producer_id} connected successfully")
                return True
            except NoBrokersAvailable:
                logger.warning(f"Producer {self.producer_id}: Kafka not ready, retrying in 2s... ({attempt + 1}/{max_retries})")
                time.sleep(2)
        raise RuntimeError(f"Producer {self.producer_id}: Failed to connect to Kafka after maximum retries")

    def _create_record_hash(self, record: Dict) -> str:
        """Create a hash for a record to detect duplicates"""
        record_str = f"{record['timestamp']}_{record['temperature']}_{record['humidity']}_{record['pressure']}_{record['location']}"
        return hashlib.md5(record_str.encode()).hexdigest()

    def _is_duplicate(self, record: Dict) -> bool:
        """Check if record is a duplicate"""
        record_hash = self._create_record_hash(record)
        if record_hash in self.record_hashes:
            return True
        self.record_hashes.add(record_hash)
        return False

    def _get_partition(self, record: Dict) -> int:
        """Determine which partition to send the record to"""
        # Use timestamp-based partitioning for better distribution
        timestamp_hash = hash(record['timestamp']) % self.partition_count
        return timestamp_hash

    def process_batch(self, batch: List[Dict]) -> None:
        """Process a batch of records with partition distribution"""
        try:
            # Filter out duplicates
            unique_records = []
            duplicates_found = 0
            
            for record in batch:
                if not self._is_duplicate(record):
                    unique_records.append(record)
                else:
                    duplicates_found += 1
            
            if duplicates_found > 0:
                logger.warning(f"Producer {self.producer_id}: Filtered out {duplicates_found} duplicate records")
            
            # Send records to appropriate partitions
            for record in unique_records:
                partition = self._get_partition(record)
                self.producer.send(
                    self.topic, 
                    value=record, 
                    partition=partition
                )
                self.partition_stats[partition] += 1
                self.processed_records += 1
                
            # Flush the batch to ensure delivery
            self.producer.flush()
            
            logger.info(f"Producer {self.producer_id}: Produced batch of {len(unique_records)} records. Total: {self.processed_records}")
            
        except Exception as e:
            logger.error(f"Producer {self.producer_id}: Error processing batch: {e}")
            raise

    async def process_data_async(self, df: pd.DataFrame) -> None:
        """Process data asynchronously with batching and partitioning"""
        self.total_records = len(df)
        logger.info(f"Producer {self.producer_id}: Starting to process {self.total_records} records in batches of {self.batch_size}")
        
        # Create data hash for integrity verification
        self.data_hash = self._create_data_hash(df)
        logger.info(f"Producer {self.producer_id}: Data integrity hash: {self.data_hash}")
        
        # Create batches
        batches = [df.iloc[i:i+self.batch_size] for i in range(0, len(df), self.batch_size)]
        
        # Process batches concurrently
        with ThreadPoolExecutor(max_workers=4) as executor:
            tasks = []
            for batch_df in batches:
                batch_records = []
                for _, row in batch_df.iterrows():
                    record = {
                        "timestamp": row["timestamp"],
                        "temperature": round(row["temperature"], 2),
                        "humidity": round(row["humidity"], 2) if pd.notna(row["humidity"]) else 0.0,
                        "pressure": round(row["pressure"], 4),
                        "location": str(row["location"]) if pd.notna(row["location"]) else "unknown",
                        "producer_id": self.producer_id  # Add producer identifier
                    }
                    # Debug: Print first record structure
                    if len(batch_records) == 0:
                        logger.info(f"Producer {self.producer_id}: First record structure: {record}")
                    batch_records.append(record)
                
                # Submit batch processing task
                task = executor.submit(self.process_batch, batch_records)
                tasks.append(task)
            
            # Wait for all tasks to complete
            for task in tasks:
                task.result()
        
        logger.info(f"Producer {self.producer_id}: Successfully processed {self.processed_records} unique records")
        logger.info(f"Producer {self.producer_id}: Partition distribution: {self.partition_stats}")

    def _create_data_hash(self, df: pd.DataFrame) -> str:
        """Create a hash of the entire dataset for integrity verification"""
        data_str = df.to_string(index=False)
        return hashlib.md5(data_str.encode()).hexdigest()

    def close(self):
        """Close the producer"""
        if self.producer:
            self.producer.close()
            logger.info(f"Producer {self.producer_id}: Closed")

def load_and_clean_data(csv_path: str) -> pd.DataFrame:
    """Load and clean CSV data"""
    logger.info(f"Loading data from {csv_path}")
    df = pd.read_csv(csv_path)
    
    # Basic cleaning
    original_count = len(df)
    df = df.dropna(subset=["temperature", "pressure"])
    df = df[df["temperature"] < 150]
    df["temperature"] = df["temperature"].astype(float)
    df["pressure"] = df["pressure"].astype(float)
    
    # Add missing columns with default values
    df["humidity"] = 0.0  # Default humidity value
    df["location"] = "unknown"  # Default location value
    
    # Normalize pressure
    df["pressure"] = (df["pressure"] - df["pressure"].min()) / (df["pressure"].max() - df["pressure"].min())
    
    cleaned_count = len(df)
    logger.info(f"Data cleaning: {original_count} → {cleaned_count} records ({original_count - cleaned_count} removed)")
    
    return df

async def run_producer(producer_id: int, data_chunk: pd.DataFrame, 
                      bootstrap_servers: str, topic: str, partition_count: int):
    """Run a single producer instance"""
    producer = ScalableKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        producer_id=producer_id,
        partition_count=partition_count,
        batch_size=1000
    )
    
    try:
        # Connect to Kafka
        producer.connect()
        
        # Process data asynchronously
        await producer.process_data_async(data_chunk)
        
        # Close producer
        producer.close()
        
        return producer.processed_records, producer.partition_stats
        
    except Exception as e:
        logger.error(f"Producer {producer_id} failed: {e}")
        raise

async def main():
    """Main execution function with multiple producers"""
    start_time = time.time()
    
    try:
        # Load and clean data
        df = load_and_clean_data("sensor_data.csv")
        
        # Configuration
        bootstrap_servers = "kafka-1:9092,kafka-2:9092,kafka-3:9092"  # Use KRaft cluster
        topic = "sensor_readings"
        partition_count = 3
        producer_count = 1  # Match the number of Kafka brokers
        
        # Split data among producers
        chunk_size = len(df) // producer_count
        data_chunks = [df.iloc[i:i+chunk_size] for i in range(0, len(df), chunk_size)]
        
        # Ensure all data is included
        if len(data_chunks) > producer_count:
            # Merge remaining chunks with the last one
            data_chunks[producer_count-1] = pd.concat(data_chunks[producer_count-1:])
            data_chunks = data_chunks[:producer_count]
        
        logger.info(f"Starting {producer_count} producers with data distribution:")
        for i, chunk in enumerate(data_chunks):
            logger.info(f"   Producer {i+1}: {len(chunk)} records")
        
        # Run producers concurrently
        tasks = []
        for i, chunk in enumerate(data_chunks):
            task = run_producer(i+1, chunk, bootstrap_servers, topic, partition_count)
            tasks.append(task)
        
        # Wait for all producers to complete
        results = await asyncio.gather(*tasks)
        
        # Calculate total statistics
        total_processed = sum(result[0] for result in results)
        total_partition_stats = {i: 0 for i in range(partition_count)}
        for result in results:
            for partition, count in result[1].items():
                total_partition_stats[partition] += count
        
        end_time = time.time()
        duration = end_time - start_time
        records_per_second = total_processed / duration
        
        logger.info(f"Multi-producer pipeline completed successfully!")
        logger.info(f"Total records processed: {total_processed}")
        logger.info(f"Total time: {duration:.2f} seconds")
        logger.info(f"Processing rate: {records_per_second:.2f} records/second")
        logger.info(f"Partition distribution: {total_partition_stats}")
        
    except Exception as e:
        logger.error(f"Multi-producer pipeline failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
