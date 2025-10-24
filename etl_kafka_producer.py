#!/usr/bin/env python3
"""
Main ETL Kafka Producer - Entry point for Docker container
This file serves as the default command for the Docker container
"""

import asyncio
import logging
from multi_producer_scalable import main

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting ETL Kafka Producer...")
    asyncio.run(main())
