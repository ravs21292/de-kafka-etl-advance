#!/usr/bin/env python3
"""
Data validation script for Kafka ETL Pipeline
"""
import asyncio
import logging
from db.async_postgres_optimized import OptimizedAsyncPostgres

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def validate_data():
    """Validate data integrity in the database"""
    db = None
    try:
        logger.info("Starting data validation...")
        
        # Initialize database connection
        db = OptimizedAsyncPostgres()
        await db.init()
        
        # Get record count
        count = await db.get_record_count()
        logger.info(f"Total records in database: {count}")
        
        # Validate data integrity
        if count > 0:
            logger.info("Data validation successful - records found in database")
            return True
        else:
            logger.warning("No records found in database")
            return False
            
    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        return False
    finally:
        if db:
            await db.close()

if __name__ == "__main__":
    success = asyncio.run(validate_data())
    exit(0 if success else 1)

