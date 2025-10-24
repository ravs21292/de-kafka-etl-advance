#!/usr/bin/env python3
"""
Complete Pipeline Runner
Handles the entire ETL pipeline from start to finish
"""

import asyncio
import logging
import time
import subprocess
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompletePipelineRunner:
    def __init__(self):
        self.compose_file = "docker-compose.yml"
        self.docker_compose = "docker-compose"
        
    def run_command(self, command: str, description: str) -> bool:
        """Run a command and return success status"""
        logger.info(f"🔄 {description}...")
        try:
            result = subprocess.run(
                command, 
                shell=True, 
                check=True, 
                capture_output=True, 
                text=True
            )
            logger.info(f"✅ {description} completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ {description} failed: {e}")
            logger.error(f"Error output: {e.stderr}")
            return False
    
    def check_services_ready(self) -> bool:
        """Check if all services are ready"""
        logger.info("🔍 Checking if services are ready...")
        
        # Check if containers are running
        result = subprocess.run(
            f"{self.docker_compose} ps",
            shell=True,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error("❌ Failed to check service status")
            return False
            
        # Check for running services
        output = result.stdout
        if "Up" not in output:
            logger.error("❌ Services are not running")
            return False
            
        logger.info("✅ All services are running")
        return True
    
    def run_complete_pipeline(self) -> bool:
        """Run the complete ETL pipeline"""
        try:
            logger.info("🚀 Starting Complete Kafka ETL Pipeline")
            logger.info("=" * 60)
            
            # Step 1: Check if services are running
            if not self.check_services_ready():
                logger.error("❌ Services are not ready. Please run 'docker-compose up -d' first.")
                return False
            
            # Step 2: Initialize pipeline (create topics)
            logger.info("📋 Step 1: Initializing Pipeline")
            if not self.run_command(
                f"{self.docker_compose} run --rm etl-service python3 init_pipeline.py",
                "Pipeline initialization"
            ):
                return False
            
            # Step 3: Clear database for fresh start
            logger.info("🗑️ Step 2: Clearing Database")
            if not self.run_command(
                f"{self.docker_compose} run --rm etl-service python3 clear_database.py",
                "Database clearing"
            ):
                return False
            
            # Step 4: Run producers
            logger.info("📤 Step 3: Running Multi-Producer Pipeline")
            if not self.run_command(
                f"{self.docker_compose} run --rm etl-service python3 multi_producer_scalable.py",
                "Multi-producer pipeline"
            ):
                return False
            
            # Step 5: Run consumers
            logger.info("📥 Step 4: Running Multi-Consumer Pipeline")
            if not self.run_command(
                f"{self.docker_compose} run --rm etl-service python3 multi_consumer_scalable.py",
                "Multi-consumer pipeline"
            ):
                return False
            
            # Step 6: Validate data
            logger.info("✅ Step 5: Validating Data")
            if not self.run_command(
                f"{self.docker_compose} run --rm etl-service python3 validate_data.py",
                "Data validation"
            ):
                return False
            
            logger.info("=" * 60)
            logger.info("🎉 Complete Pipeline Executed Successfully!")
            logger.info("📊 Check the logs above for detailed statistics")
            logger.info("🔍 You can also check the database with: make shell")
            logger.info("📈 Monitor with Kafka UI at: http://localhost:8080")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline execution failed: {e}")
            return False

def main():
    """Main function"""
    runner = CompletePipelineRunner()
    success = runner.run_complete_pipeline()
    
    if not success:
        logger.error("❌ Pipeline failed. Check the logs above for details.")
        sys.exit(1)
    else:
        logger.info("✅ Pipeline completed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()
