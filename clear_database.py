#!/usr/bin/env python3
"""
Script to clear the database and reset sequences
"""
import asyncio
import asyncpg

POSTGRES_CONFIG = {
    "user": "postgres",
    "password": "password",
    "host": "postgres",
    "port": 5432,
    "database": "sensors"
}

async def clear_database():
    """Clear all data from the sensors table"""
    try:
        # Connect to database
        conn = await asyncpg.connect(**POSTGRES_CONFIG)
        
        # Clear all data
        await conn.execute("TRUNCATE TABLE sensors RESTART IDENTITY CASCADE;")
        
        # Verify the table is empty
        count = await conn.fetchval("SELECT COUNT(*) FROM sensors")
        
        print(f"Database cleared successfully!")
        print(f"Records in database: {count}")
        
        await conn.close()
        
    except Exception as e:
        print(f"Error clearing database: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(clear_database())
