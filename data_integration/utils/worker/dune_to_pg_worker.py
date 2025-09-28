# Standard imports
import json
import logging
import typing
from datetime import datetime

# Library imports
import pandas as pd
import sqlalchemy

# Local imports
from . import DuneExtractor, PgLoader
from data_integration.arguments import FULL_REFRESH, INCREMENTAL_VALUE

class DuneToPgWorker(object):
    def __init__(
        self, 
        dune_extractor: DuneExtractor,
        target_schema_name: str,
        target_table: str,
        target_con: sqlalchemy.engine.Connection = None, 
    ):
        self.dune_extractor = dune_extractor
        self.target_schema_name = target_schema_name
        self.target_table = target_table
        self.target_con = target_con

    def fetch(
        self,
        query_id: int,
        query_parameters: str = None,
        max_wait_time: int = 300,
    ):
        """
        Fetch data from Dune Analytics using query ID and parameters.
        
        Args:
            query_id (int): Dune query ID to execute
            query_parameters (str): Parameters to pass to the query
            max_wait_time (int): Maximum wait time for query execution
            
        Returns:
            List[Dict]: Raw data from Dune query
        """
        logger = logging.getLogger(f'{self.target_schema_name}.{self.target_table}')
        try:
            # Execute the query and get execution ID
            execution_id = self.dune_extractor.execute_query(
                query_id=query_id,
                parameters=query_parameters
            )
            
            # Get the results
            raw_data = self.dune_extractor.get_results(
                execution_id=execution_id,
                max_wait_time=max_wait_time
            )
            
            logger.info(f'Successfully fetched {len(raw_data)} rows from Dune query {query_id}')
            return raw_data
            
        except Exception as e:
            logger.error(f'Error fetching data from Dune query {query_id}: {e}')
            raise

    def load_to_postgres(
        self,
        data: typing.List[typing.Dict],
        source_unique_keys: typing.List[str] = None,
        incremental_column: str = None,
        incremental_value: typing.Any = None,
        load_strategy: str = FULL_REFRESH
    ):
        """
        Load data to PostgreSQL database.
        
        Args:
            data (List[Dict]): Data to load
            source_unique_keys (List[str]): Unique key columns for upsert
            incremental_column (str): Column for incremental loading
            incremental_value: Value for incremental filtering
            load_strategy (str): Loading strategy (FULL_REFRESH or INCREMENTAL_VALUE)
        """
        logger = logging.getLogger(f'{self.target_schema_name}.{self.target_table}')
        
        if not data:
            logger.warning("No data to load")
            return
            
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            logger.info(f'Converted {len(df)} rows to DataFrame')
            
            # Initialize PgLoader (assuming it exists)
            pg_loader = PgLoader(
                connection=self.target_con,
                schema_name=self.target_schema_name,
                table_name=self.target_table
            )
            
            # Load data based on strategy
            if load_strategy == FULL_REFRESH:
                pg_loader.load_full_refresh(df)
            else:
                pg_loader.load_incremental(
                    df=df,
                    unique_keys=source_unique_keys,
                    incremental_column=incremental_column,
                    incremental_value=incremental_value
                )
                
            logger.info(f'Successfully loaded data to {self.target_schema_name}.{self.target_table}')
            
        except Exception as e:
            logger.error(f'Error loading data to PostgreSQL: {e}')
            raise