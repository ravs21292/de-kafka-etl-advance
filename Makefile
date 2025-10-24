# Kafka ETL Pipeline with KRaft Mode - Makefile
# This Makefile provides convenient commands for managing the pipeline

.PHONY: help up down restart logs status clean init producer consumer validate full-pipeline

# Default target
help:
	@echo "🚀 Kafka ETL Pipeline with KRaft Mode - Available Commands:"
	@echo ""
	@echo "📦 Infrastructure:"
	@echo "  make up          - Start all services (Kafka cluster + PostgreSQL + Kafka UI)"
	@echo "  make down        - Stop all services"
	@echo "  make restart     - Restart all services"
	@echo "  make logs        - Show logs from all services"
	@echo "  make status      - Show status of all services"
	@echo "  make clean       - Stop services and remove volumes"
	@echo ""
	@echo "🔧 Pipeline Management:"
	@echo "  make init        - Initialize pipeline (create topics)"
	@echo "  make producer    - Run multi-producer pipeline"
	@echo "  make consumer    - Run multi-consumer pipeline"
	@echo "  make validate    - Validate data in database"
	@echo "  make full-pipeline - Run complete pipeline (init + producer + consumer + validate)"
	@echo ""
	@echo "📈 Consumer Scaling:"
	@echo "  make consumer-1  - Test with 1 consumer"
	@echo "  make consumer-2  - Test with 2 consumers"
	@echo "  make consumer-3  - Test with 3 consumers"
	@echo "  make consumer-6  - Test with 6 consumers"
	@echo "  make perf-comparison - Compare all consumer configurations"
	@echo ""
	@echo "📊 Monitoring:"
	@echo "  make kafka-ui    - Open Kafka UI in browser"
	@echo "  make db-connect  - Connect to PostgreSQL database"
	@echo "  make monitor     - System monitoring dashboard"
	@echo "  make detailed-monitor - Detailed system monitoring"
	@echo "  make architecture - Show system architecture"
	@echo ""

# Infrastructure commands
up:
	@echo "🚀 Starting Kafka ETL Pipeline with KRaft mode..."
	docker-compose up -d
	@echo "⏳ Waiting for services to be ready..."
	@sleep 30
	@echo "✅ Services started! Kafka UI available at http://localhost:8080"

down:
	@echo "🛑 Stopping all services..."
	docker-compose down

restart: down up

logs:
	@echo "📋 Showing logs from all services..."
	docker-compose logs -f

status:
	@echo "📊 Service Status:"
	docker-compose ps

clean:
	@echo "🧹 Cleaning up services and volumes..."
	docker-compose down -v
	docker-compose rm -f

# Pipeline management commands
init:
	@echo "🔧 Initializing pipeline (creating topics)..."
	docker-compose run --rm etl-service python3 init_pipeline.py || echo "⚠️ Topic may already exist, continuing..."

producer:
	@echo "📤 Running multi-producer pipeline..."
	docker-compose run --rm etl-service python3 multi_producer_scalable.py

consumer:
	@echo "📥 Running multi-consumer pipeline..."
	docker-compose run --rm etl-service python3 multi_consumer_scalable.py

validate:
	@echo "✅ Validating data in database..."
	docker-compose run --rm etl-service python3 validate_data.py

full-pipeline: init producer consumer validate
	@echo "🎉 Complete pipeline executed successfully!"

# Monitoring commands
kafka-ui:
	@echo "🌐 Opening Kafka UI in browser..."
	@echo "Kafka UI is available at: http://localhost:8080"
	@echo "Press Ctrl+C to stop this command"
	@while true; do sleep 1; done

db-connect:
	@echo "🗄️ Connecting to PostgreSQL database..."
	docker-compose exec postgres psql -U kafka_user -d kafka_etl_db

# Development commands
shell:
	@echo "🐚 Opening shell in ETL service container..."
	docker-compose run --rm etl-service /bin/bash

# Quick test commands
test-producer:
	@echo "🧪 Testing producer with 1000 records..."
	docker-compose run --rm etl-service python3 -c "from multi_producer_scalable import MultiProducerPipeline; p = MultiProducerPipeline(); p.run_producer(1000)"

test-consumer:
	@echo "🧪 Testing consumer..."
	timeout 30 docker-compose run --rm etl-service python3 multi_consumer_scalable.py

# Performance testing
perf-test:
	@echo "⚡ Running performance test..."
	@echo "Starting producer..."
	docker-compose run --rm etl-service python3 multi_producer_scalable.py &
	@sleep 5
	@echo "Starting consumer..."
	timeout 60 docker-compose run --rm etl-service python3 multi_consumer_scalable.py
	@echo "Performance test completed!"

# Multi-consumer performance test
perf-test-multi:
	@echo "⚡ Running Multi-Consumer Performance Test..."
	@echo "Configuration: 3 consumers, 3 partitions"
	@echo "Starting producer..."
	docker-compose run --rm etl-service python3 multi_producer_scalable.py &
	@sleep 5
	@echo "Starting 3 consumers..."
	timeout 60 docker-compose run --rm etl-service python3 multi_consumer_scalable.py
	@echo "Multi-consumer performance test completed!"

# Consumer scaling test
scale-test:
	@echo "📈 Consumer Scaling Test"
	@echo "========================"
	@echo "Testing with different consumer counts..."
	@echo ""
	@echo "🔧 Current configuration: 3 consumers, 3 partitions"
	@echo "Expected: Each consumer handles 1 partition"
	@echo ""
	@make perf-test-multi

# Flexible consumer testing
consumer-1:
	@echo "🧪 Testing with 1 consumer..."
	CONSUMER_COUNT=1 timeout 30 docker-compose run --rm etl-service python3 multi_consumer_scalable.py

consumer-2:
	@echo "🧪 Testing with 2 consumers..."
	CONSUMER_COUNT=2 timeout 30 docker-compose run --rm etl-service python3 multi_consumer_scalable.py

consumer-3:
	@echo "🧪 Testing with 3 consumers..."
	CONSUMER_COUNT=3 timeout 30 docker-compose run --rm etl-service python3 multi_consumer_scalable.py

consumer-6:
	@echo "🧪 Testing with 6 consumers..."
	CONSUMER_COUNT=6 timeout 30 docker-compose run --rm etl-service python3 multi_consumer_scalable.py

# Performance comparison
perf-comparison:
	@echo "📊 Performance Comparison Test"
	@echo "=============================="
	@echo "Testing different consumer configurations..."
	@echo ""
	@echo "1️⃣ Single Consumer Test:"
	@make consumer-1
	@echo ""
	@echo "2️⃣ Two Consumers Test:"
	@make consumer-2
	@echo ""
	@echo "3️⃣ Three Consumers Test:"
	@make consumer-3
	@echo ""
	@echo "6️⃣ Six Consumers Test:"
	@make consumer-6
	@echo ""
	@echo "✅ Performance comparison completed!"

# Health check
health:
	@echo "🏥 Checking system health..."
	@echo "Kafka cluster status:"
	@docker-compose exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
	@echo "PostgreSQL status:"
	@docker-compose exec postgres pg_isready -U kafka_user
	@echo "Kafka UI status:"
	@curl -s http://localhost:8080/api/health > /dev/null && echo "✅ Kafka UI is healthy" || echo "❌ Kafka UI is not responding"

# Monitoring commands
monitor:
	@echo "📊 System Monitoring Dashboard"
	@echo "================================"
	@echo "🔧 Infrastructure Status:"
	@make status
	@echo ""
	@echo "📈 Kafka Cluster Info:"
	@make kafka-info
	@echo ""
	@echo "👥 Consumer Groups:"
	@make consumer-groups
	@echo ""
	@echo "📊 Topic Details:"
	@make topic-details
	@echo ""
	@echo "🗄️ Database Stats:"
	@make db-stats

kafka-info:
	@echo "🔍 Kafka Cluster Information:"
	@echo "Brokers: 3 (kafka-1, kafka-2, kafka-3)"
	@echo "Partitions per topic: 3"
	@echo "Replication factor: 3"
	@echo "Consumer groups: 1 (scalable_etl_group)"
	@echo "Producers: 1 (configurable)"
	@echo "Consumers: 1 (configurable)"

consumer-groups:
	@echo "👥 Consumer Groups and Members:"
	@docker-compose exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --list || echo "No consumer groups found"
	@echo ""
	@echo "📊 Consumer Group Details:"
	@docker-compose exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group scalable_etl_group || echo "Consumer group not found"

topic-details:
	@echo "📋 Topic Information:"
	@docker-compose exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic sensor_readings || echo "Topic not found"
	@echo ""
	@echo "📊 Topic Offsets:"
	@docker-compose exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic sensor_readings || echo "Unable to get offsets"

db-stats:
	@echo "🗄️ Database Statistics:"
	@docker-compose exec postgres psql -U kafka_user -d kafka_etl_db -c "SELECT COUNT(*) as total_records FROM sensors;" || echo "Unable to connect to database"
	@echo ""
	@echo "📈 Database Performance:"
	@docker-compose exec postgres psql -U kafka_user -d kafka_etl_db -c "SELECT schemaname, tablename, attname, n_distinct, correlation FROM pg_stats WHERE tablename = 'sensors';" || echo "Unable to get stats"

# Detailed monitoring
detailed-monitor:
	@echo "🔍 Detailed System Monitoring"
	@echo "=============================="
	@echo "🐳 Container Resource Usage:"
	@docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"
	@echo ""
	@echo "📊 Kafka Broker Details:"
	@docker-compose exec kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092 || echo "Unable to get broker info"
	@echo ""
	@echo "🔄 Consumer Lag:"
	@docker-compose exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group scalable_etl_group --members --verbose || echo "Unable to get consumer lag"

# Performance testing
perf-monitor:
	@echo "⚡ Performance Monitoring"
	@echo "========================="
	@echo "🚀 Starting performance test with monitoring..."
	@echo "Producer: 1 instance, 1000 records"
	@echo "Consumer: 1 instance, batch size 1000"
	@echo "Brokers: 3 (KRaft mode)"
	@echo ""
	@make test-producer &
	@sleep 5
	@make test-consumer
	@echo "Performance test completed!"

# System architecture info
architecture:
	@echo "🏗️ System Architecture Overview"
	@echo "================================"
	@echo "📊 Current Configuration:"
	@echo "  • Brokers: 3 (kafka-1:9092, kafka-2:9092, kafka-3:9092)"
	@echo "  • Mode: KRaft (no Zookeeper)"
	@echo "  • Partitions: 3 per topic"
	@echo "  • Replication: 3x"
	@echo "  • Producers: 1 (configurable in multi_producer_scalable.py)"
	@echo "  • Consumers: 1 (configurable in multi_consumer_scalable.py)"
	@echo "  • Consumer Group: scalable_etl_group"
	@echo "  • Database: PostgreSQL 15"
	@echo "  • Batch Size: 1000 records"
	@echo ""
	@echo "🔄 Data Flow:"
	@echo "  CSV → Producer → Kafka (3 partitions) → Consumer → PostgreSQL"
	@echo ""
	@echo "🔧 Scaling Options:"
	@echo "  • Increase producers: Edit producer_count in multi_producer_scalable.py"
	@echo "  • Increase consumers: Edit consumer_count in multi_consumer_scalable.py"
	@echo "  • Add more brokers: Add to docker-compose.yml"
	@echo "  • Increase partitions: Update KAFKA_NUM_PARTITIONS"
