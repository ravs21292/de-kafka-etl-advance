import asyncpg
import logging
from typing import List, Dict
import asyncio

POSTGRES_CONFIG = {
    "user": "postgres",
    "password": "password",
    "host": "postgres",   # Use Docker service name when running in Docker
    "port": 5432,         # Internal port when running in Docker
    "database": "sensors"
}

logger = logging.getLogger(__name__)

class OptimizedAsyncPostgres:
    def __init__(self):
        self.pool = None

    async def _init_pool(self):
        """Initialize connection pool with optimized settings"""
        self.pool = await asyncpg.create_pool(
            **POSTGRES_CONFIG,
            min_size=5,           # Increased minimum connections
            max_size=20,          # Increased maximum connections
            max_queries=50000,    # Max queries per connection
            max_inactive_connection_lifetime=300,  # 5 minutes
            command_timeout=60,   # 60 second command timeout
            server_settings={
                'jit': 'off'      # Disable JIT for better performance
            }
        )
        
        # Database schema will be initialized when first accessed
            
        logger.info("Database connection pool initialized")

    async def init(self):
        """Initialize database connection pool and schema"""
        if self.pool is None:
            await self._init_pool()
        
        # Initialize database schema
        async with self.pool.acquire() as conn:
            # Create table if it doesn't exist
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sensors (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL,
                    temperature FLOAT NOT NULL,
                    humidity FLOAT NOT NULL,
                    pressure FLOAT NOT NULL,
                    location VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create indexes for better query performance - use separate transactions
            try:
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_sensors_timestamp ON sensors(timestamp);")
                logger.info("Created timestamp index")
            except Exception as e:
                if "already exists" not in str(e) and "duplicate key" not in str(e):
                    logger.warning(f"Index creation warning: {e}")
            
            try:
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_sensors_created_at ON sensors(created_at);")
                logger.info("Created created_at index")
            except Exception as e:
                if "already exists" not in str(e) and "duplicate key" not in str(e):
                    logger.warning(f"Index creation warning: {e}")

    async def insert_batch(self, records: List[Dict]) -> None:
        """Insert batch of records using COPY for maximum performance"""
        if not records:
            return
            
        try:
            async with self.pool.acquire() as conn:
                # Prepare data for COPY with proper datetime conversion
                data = []
                for record in records:
                    # Convert string timestamp to datetime if needed
                    timestamp = record["timestamp"]
                    if isinstance(timestamp, str):
                        from datetime import datetime
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    
                    data.append((
                        timestamp,
                        float(record["temperature"]),
                        float(record["humidity"]),
                        float(record["pressure"]),
                        str(record["location"])
                    ))
                
                # Use COPY for bulk insert (fastest method)
                await conn.copy_records_to_table(
                    'sensors',
                    records=data,
                    columns=('timestamp', 'temperature', 'humidity', 'pressure', 'location')
                )
                
        except Exception as e:
            logger.error(f"Database insert error: {e}")
            raise

    async def get_record_count(self) -> int:
        """Get total record count for validation"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval("SELECT COUNT(*) FROM sensors")
            return result

    async def validate_data_integrity(self, expected_count: int) -> bool:
        """Validate that all data was inserted correctly"""
        try:
            actual_count = await self.get_record_count()
            logger.info(f"Data validation: Expected {expected_count}, Actual {actual_count}")
            return actual_count >= expected_count
        except Exception as e:
            logger.error(f"Data validation error: {e}")
            return False

    async def close(self):
        """Close the connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
