# Standard imports
import logging
import os
import traceback
from typing import Dict, List, Any

# Library imports
import sqlalchemy
from sqlalchemy import create_engine

# Local imports
from ..pull_raw.utils import get_tables_to_sync
from data_integration.utils.utils import get_abspath
from data_integration.utils.worker.dune_extractor import DuneExtractor
from data_integration.utils.worker.dune_to_pg_worker import DuneToPgWorker
from data_integration.arguments import FULL_REFRESH, INCREMENTAL_VALUE

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
TARGET_SCHEMA_NAME = 'bitcoin'

class DuneBitcoinPipeline:
    """Main pipeline class for Dune Analytics Bitcoin data extraction."""
    
    def __init__(self, target_session: sqlalchemy.orm.Session = None):
        """
        Initialize the pipeline.
        
        Args:
            target_session: SQLAlchemy session for target database
        """
        self.target_session = target_session
        self.target_schema_name = TARGET_SCHEMA_NAME
        
        # Initialize Dune components
        self.dune_extractor = DuneExtractor(
            api_key=os.environ.get('DUNE_API_KEY'),
        )
        
        self.dune_to_pg_worker = DuneToPgWorker(
            dune_extractor=self.dune_extractor,
            target_schema_name=self.target_schema_name,
            target_table="",  # Will be set per table
            target_con=self.target_session.get_bind() if target_session else None,
        )
        
    def sync_table_full_refresh(
        self, 
        table_name: str, 
        query_id: int, 
        source_unique_keys: List[str] = None,
        query_parameters: str = None
    ):
        """Sync table using full refresh strategy."""
        logger.info(f"Starting full refresh sync for table: {table_name}")
        
        # Update worker's target table
        self.dune_to_pg_worker.target_table = table_name
        
        try:
            self.dune_to_pg_worker.run(
                query_id=query_id,
                query_parameters=query_parameters,
                source_unique_keys=source_unique_keys or ['id'],
                load_strategy=FULL_REFRESH,
                max_wait_time=300
            )
            logger.info(f"Successfully completed full refresh for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to sync table {table_name}: {e}")
            raise
            
    def sync_table_incremental(
        self, 
        table_name: str, 
        query_id: int,
        incremental_column: str = 'date',
        source_unique_keys: List[str] = None
    ):
        """Sync table using incremental strategy."""
        logger.info(f"Starting incremental sync for table: {table_name}")
        
        # Update worker's target table
        self.dune_to_pg_worker.target_table = table_name
        
        try:
            # Get last incremental value from existing data
            from data_integration.utils.worker.pg_loader import PgLoader
            pg_loader = PgLoader(
                connection=self.target_session.get_bind(),
                schema_name=self.target_schema_name,
                table_name=table_name
            )
            
            last_value = pg_loader.get_max_value(incremental_column)
            
            # If no previous data, do full refresh
            if last_value is None:
                logger.info(f"No previous data found for {table_name}, doing full refresh")
                self.sync_table_full_refresh(
                    table_name=table_name,
                    query_id=query_id,
                    source_unique_keys=source_unique_keys
                )
                return
                
            # Use last value as parameter for incremental sync
            query_parameters = str(last_value) if last_value else None
            
            self.dune_to_pg_worker.fetch(
                query_id=query_id,
                query_parameters=query_parameters,
                source_unique_keys=source_unique_keys or ['id'],
                incremental_column=incremental_column,
                incremental_value=last_value,
                load_strategy=INCREMENTAL_VALUE,
                max_wait_time=300
            )
            logger.info(f"Successfully completed incremental sync for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to sync table {table_name} incrementally: {e}")
            raise
            
    def run_pipeline(self, table_meta_data: Dict):
        """Run the complete pipeline."""
        logger.info("Starting Dune Bitcoin data pipeline")
        
        try:
            # Load tables configuration
            tables_to_sync = get_tables_to_sync(table_meta_data)
            logger.info(f"Found {len(tables_to_sync)} tables to sync")
            
            # Process each table from YAML config
            for table in tables_to_sync:
                table_name = table.get('name')
                query_id = table.get('id')
                sync_type = table.get('sync_type')
                source_unique_keys = table.get('source_unique_keys', ['id'])
                incremental_column = table.get('incremental_column', 'updated_at')
                
                logger.info(f"Processing table: {table_name} (sync_type: {sync_type})")
                
                try:
                    if sync_type == 'full_refresh':
                        self.sync_table_full_refresh(
                            table_name=table_name,
                            query_id=query_id,
                            source_unique_keys=source_unique_keys,
                            query_parameters=table.get('query_parameters')
                        )
                        
                    elif sync_type is None or sync_type == 'sync_incremental':
                        self.sync_table_incremental(
                            table_name=table_name,
                            query_id=query_id,
                            incremental_column=incremental_column,
                            source_unique_keys=source_unique_keys
                        )
                        
                    else:
                        raise ValueError(f'Invalid sync_type "{sync_type}" for table {table_name}')
                        
                    logger.info(f"✅ Successfully processed table: {table_name}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process table {table_name}: {e}")
                    logger.error(traceback.format_exc())
                    # Continue with next table instead of stopping entire pipeline
                    continue
                    
            logger.info("🎉 Pipeline execution completed")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            logger.error(traceback.format_exc())
            raise

# Main execution function
def main(target_session, table_meta_data):
    """Main entry point."""
    pipeline = DuneBitcoinPipeline(target_session=target_session)
    pipeline.run_pipeline(table_meta_data)
