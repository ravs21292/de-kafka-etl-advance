#!/usr/bin/env python3
"""
Pipeline Initialization Script
Creates topics and initializes the complete ETL pipeline
"""

import asyncio
import logging
import time
from kafka_topic_manager import KafkaTopicManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def init_pipeline():
    """Initialize the complete ETL pipeline"""
    try:
        logger.info("🚀 Initializing Kafka ETL Pipeline...")
        
        # Configuration
        bootstrap_servers = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        topic_name = "sensor_readings"
        
        # Create topic manager
        manager = KafkaTopicManager(bootstrap_servers)
        
        # Connect to Kafka
        logger.info("📡 Connecting to Kafka cluster...")
        manager.connect()
        
        # List existing topics
        logger.info("📋 Checking existing topics...")
        manager.list_topics()
        
        # Create topic with proper configuration
        logger.info(f"🔧 Creating topic '{topic_name}' with 3 partitions and 3 replicas...")
        manager.create_topic(topic_name, num_partitions=3, replication_factor=3)
        
        # Wait for topic to be ready
        logger.info("⏳ Waiting for topic to be ready...")
        time.sleep(5)
        
        # Describe the created topic
        logger.info("📊 Verifying topic configuration...")
        manager.describe_topic(topic_name)
        
        # Close connection
        manager.close()
        
        logger.info("✅ Pipeline initialization completed successfully!")
        logger.info("🎯 Ready to run producers and consumers")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline initialization failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(init_pipeline())
    if not success:
        exit(1)
