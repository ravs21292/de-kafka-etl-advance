# Kafka ETL Pipeline with KRaft Mode

This project is part of my backend engineering portfolio. A scalable, high-performance ETL pipeline using Apache Kafka in KRaft mode (no Zookeeper required) with PostgreSQL for data storage.

## 🏗️ Architecture

```
CSV Data → Multi-Producers → Kafka KRaft Cluster → Multi-Consumers → PostgreSQL
    ↓           ↓                    ↓                    ↓            ↓
sensor_data.csv  (3 Brokers)    (3 Partitions)      (Batch Processing)  (Optimized Storage)
```

## 📋 Prerequisites

- Docker and Docker Compose
- 4GB+ RAM recommended

## 🚀 Quick Start

### 1. Start Services
```bash
# Start all services (Kafka cluster + PostgreSQL + Kafka UI)
docker-compose up -d

# Wait for services to be ready (30 seconds)
sleep 30
```

### 2. Run Complete Pipeline
```bash
# Run the complete ETL pipeline (init + producer + consumer + validation)
docker-compose run --rm etl-service python3 run_complete_pipeline.py
```

### 3. Monitor Results
- **Kafka UI**: http://localhost:8080
- **PostgreSQL**: localhost:5433 (user: postgres, password: password, db: sensors)

## 🔧 Manual Operations

### Individual Components
```bash
# Initialize pipeline (create topics)
docker-compose run --rm etl-service python3 init_pipeline.py

# Clear database
docker-compose run --rm etl-service python3 clear_database.py

# Run producers only
docker-compose run --rm etl-service python3 multi_producer_scalable.py

# Run consumers only
docker-compose run --rm etl-service python3 multi_consumer_scalable.py

# Validate data
docker-compose run --rm etl-service python3 validate_data.py
```

### Database Operations
```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U postgres -d sensors

# Check data
SELECT COUNT(*) FROM sensors;
SELECT * FROM sensors LIMIT 10;
```

## 📊 Pipeline Flow

1. **Data Source**: `sensor_data.csv` (100K+ sensor records)
2. **Extract**: Load and clean CSV data using Pandas
3. **Transform**: Add location/humidity fields, normalize pressure
4. **Load**: Stream to Kafka with partition distribution
5. **Consume**: Multi-consumer batch processing (1000 records/batch)
6. **Store**: Optimized PostgreSQL storage with indexing
7. **Validate**: Data integrity verification

## 🗂️ Project Structure

## ⚙️ Configuration

### Kafka Setup
- **Brokers**: 3-node KRaft cluster (kafka-1, kafka-2, kafka-3)
- **Ports**: 9092, 9094, 9095 (external), 9092 (internal)
- **Topic**: `sensor_readings` (3 partitions, 3 replicas)
- **Replication**: 3x for fault tolerance

### Performance Settings
- **Batch Size**: 1000 records per batch
- **Compression**: GZIP for message compression
- **Idempotence**: Enabled to prevent duplicates
- **Connection Pooling**: 5-20 PostgreSQL connections

## 🔍 Monitoring

### Kafka UI Features
- Real-time topic monitoring
- Message browsing
- Consumer group management
- Broker health status

### Logs
```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f kafka-1
docker-compose logs -f etl-service
```

## 🛠️ Troubleshooting

### Common Issues
1. **Kafka not ready**: Wait for all brokers to start (check logs)
2. **Connection refused**: Ensure all services are running
3. **Topic creation fails**: Check broker connectivity
4. **Database connection issues**: Verify PostgreSQL is running

### Health Checks
```bash
# Check Kafka cluster health
docker-compose exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

# Check database connectivity
docker-compose exec postgres pg_isready -U postgres
```

## 🚀 Scaling

### Horizontal Scaling
- **Producers**: Increase `producer_count` in scripts
- **Consumers**: Increase `consumer_count` in scripts
- **Kafka Brokers**: Add more brokers to docker-compose.yml

### Performance Metrics
- **Throughput**: 10,000+ records/second
- **Latency**: <100ms end-to-end
- **Memory Usage**: <512MB per service

## 🧹 Cleanup

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clears all data)
docker-compose down -v
```

---

**Note**: This setup uses KRaft mode, eliminating the need for Zookeeper and providing a more modern, efficient Kafka deployment.
