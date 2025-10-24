#!/usr/bin/env python3
"""
Kafka Topic Management Script for Scalable ETL Pipeline
Creates and manages topics with proper partitioning and replication
"""

import sys
import time
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka.admin.config_resource import ConfigResource
from kafka.errors import TopicAlreadyExistsError, KafkaError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaTopicManager:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = None
        
    def connect(self, max_retries: int = 10):
        """Connect to Kafka admin client"""
        for attempt in range(max_retries):
            try:
                self.admin_client = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id='topic_manager'
                )
                logger.info("✅ Connected to Kafka admin client")
                return True
            except Exception as e:
                logger.warning(f"Kafka not ready, retrying in 5s... ({attempt + 1}/{max_retries})")
                time.sleep(5)
        raise RuntimeError("Failed to connect to Kafka admin client")

    def create_topic(self, topic_name: str, num_partitions: int = 3, replication_factor: int = 3):
        """Create a topic with specified partitions and replication"""
        from kafka.admin import NewTopic
        
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs={
                    'cleanup.policy': 'delete',
                    'retention.ms': '604800000',  # 7 days
                    'segment.ms': '86400000',     # 1 day
                    'compression.type': 'gzip'
                }
            )
            
            # Create the topic
            result = self.admin_client.create_topics([topic])
            
            # Wait for topic creation
            for topic_name, future in result.items():
                try:
                    future.result()
                    logger.info(f"✅ Topic '{topic_name}' created successfully")
                    logger.info(f"   Partitions: {num_partitions}")
                    logger.info(f"   Replication Factor: {replication_factor}")
                except TopicAlreadyExistsError:
                    logger.info(f"ℹ️  Topic '{topic_name}' already exists")
                except Exception as e:
                    logger.error(f"❌ Failed to create topic '{topic_name}': {e}")
                    raise
                    
        except Exception as e:
            logger.error(f"❌ Error creating topic: {e}")
            raise

    def list_topics(self):
        """List all topics"""
        try:
            metadata = self.admin_client.list_topics()
            logger.info("📋 Available topics:")
            for topic in metadata:
                logger.info(f"   - {topic}")
        except Exception as e:
            logger.error(f"❌ Error listing topics: {e}")

    def describe_topic(self, topic_name: str):
        """Describe topic details"""
        try:
            metadata = self.admin_client.describe_topics([topic_name])
            if topic_name in metadata:
                topic_metadata = metadata[topic_name]
                logger.info(f"📊 Topic '{topic_name}' details:")
                logger.info(f"   Partitions: {len(topic_metadata.partitions)}")
                for partition_id, partition in topic_metadata.partitions.items():
                    logger.info(f"   Partition {partition_id}: Leader={partition.leader}, Replicas={partition.replicas}")
            else:
                logger.warning(f"⚠️  Topic '{topic_name}' not found")
        except Exception as e:
            logger.error(f"❌ Error describing topic: {e}")

    def delete_topic(self, topic_name: str):
        """Delete a topic"""
        try:
            result = self.admin_client.delete_topics([topic_name])
            for topic, future in result.items():
                try:
                    future.result()
                    logger.info(f"✅ Topic '{topic}' deleted successfully")
                except Exception as e:
                    logger.error(f"❌ Failed to delete topic '{topic}': {e}")
        except Exception as e:
            logger.error(f"❌ Error deleting topic: {e}")

    def close(self):
        """Close admin client"""
        if self.admin_client:
            self.admin_client.close()
            logger.info("🔒 Admin client closed")

def main():
    """Main function"""
    bootstrap_servers = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    topic_name = "sensor_readings"
    
    manager = KafkaTopicManager(bootstrap_servers)
    
    try:
        # Connect to Kafka
        manager.connect()
        
        # List existing topics
        manager.list_topics()
        
        # Create topic with 3 partitions and 3 replicas
        manager.create_topic(topic_name, num_partitions=3, replication_factor=3)
        
        # Wait a moment for topic to be ready
        time.sleep(2)
        
        # Describe the created topic
        manager.describe_topic(topic_name)
        
        logger.info("🎉 Topic management completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Topic management failed: {e}")
        sys.exit(1)
    finally:
        manager.close()

if __name__ == "__main__":
    main()
