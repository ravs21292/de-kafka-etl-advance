#!/usr/bin/env python3
"""
Simple Kafka Topic Creation Script
"""

import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_topic():
    """Create the sensor_readings topic with 3 partitions and 3 replicas"""
    bootstrap_servers = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='topic_creator'
        )
        
        # Create topic
        topic = NewTopic(
            name="sensor_readings",
            num_partitions=3,
            replication_factor=3,
            topic_configs={
                'cleanup.policy': 'delete',
                'retention.ms': '604800000',  # 7 days
                'segment.ms': '86400000',     # 1 day
                'compression.type': 'gzip'
            }
        )
        
        # Create the topic
        result = admin_client.create_topics([topic])
        
        # Wait for topic creation
        for topic_name, future in result.items():
            try:
                future.result()
                print(f"Topic '{topic_name}' created successfully")
                print(f"   Partitions: 3")
                print(f"   Replication Factor: 3")
            except TopicAlreadyExistsError:
                print(f"Topic '{topic_name}' already exists")
            except Exception as e:
                print(f"Failed to create topic '{topic_name}': {e}")
                raise
        
        admin_client.close()
        print("Topic creation completed successfully!")
        
    except Exception as e:
        print(f"Topic creation failed: {e}")
        raise

if __name__ == "__main__":
    create_topic()
