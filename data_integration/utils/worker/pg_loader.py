# Standard imports
import logging
import typing
from datetime import datetime

# Library imports
import pandas as pd
import sqlalchemy
from sqlalchemy import text, MetaData, Table, Column, String, Integer, DateTime, Boolean, Float
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError


class PgLoader:
    """
    PostgreSQL data loader with support for full refresh and incremental loading.
    """
    
    def __init__(
        self,
        connection: sqlalchemy.engine.Connection,
        schema_name: str,
        table_name: str
    ):
        """
        Initialize PgLoader.
        
        Args:
            connection: SQLAlchemy connection object
            schema_name: Target schema name
            table_name: Target table name
        """
        self.connection = connection
        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{schema_name}.{table_name}"
        self.logger = logging.getLogger(f'{schema_name}.{table_name}')
        
    def _create_schema_if_not_exists(self):
        """Create schema if it doesn't exist."""
        try:
            self.connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"))
            self.connection.commit()
            self.logger.info(f"Schema {self.schema_name} created or already exists")
        except SQLAlchemyError as e:
            self.logger.error(f"Error creating schema {self.schema_name}: {e}")
            raise
            
    def _infer_column_type(self, series: pd.Series) -> typing.Any:
        """
        Infer SQLAlchemy column type from pandas Series.
        
        Args:
            series: Pandas Series to infer type from
            
        Returns:
            SQLAlchemy column type
        """
        if pd.api.types.is_integer_dtype(series):
            return Integer
        elif pd.api.types.is_float_dtype(series):
            return Float
        elif pd.api.types.is_bool_dtype(series):
            return Boolean
        elif pd.api.types.is_datetime64_any_dtype(series):
            return DateTime
        else:
            return String
            
    def _create_table_from_dataframe(self, df: pd.DataFrame, if_not_exists: bool = True):
        """
        Create table based on DataFrame structure.
        
        Args:
            df: DataFrame to create table from
            if_not_exists: Whether to use IF NOT EXISTS clause
        """
        try:
            metadata = MetaData()
            
            # Create columns based on DataFrame
            columns = []
            for col_name in df.columns:
                col_type = self._infer_column_type(df[col_name])
                columns.append(Column(col_name, col_type))
                
            # Create table object
            table = Table(
                self.table_name,
                metadata,
                *columns,
                schema=self.schema_name
            )
            
            # Create table in database
            if if_not_exists:
                table.create(self.connection.engine, checkfirst=True)
            else:
                table.create(self.connection.engine, checkfirst=False)
                
            self.logger.info(f"Table {self.full_table_name} created successfully")
            
        except SQLAlchemyError as e:
            self.logger.error(f"Error creating table {self.full_table_name}: {e}")
            raise
            
    def _table_exists(self) -> bool:
        """Check if table exists in the database."""
        try:
            query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = :schema_name 
                    AND table_name = :table_name
                )
            """)
            result = self.connection.execute(
                query, 
                {"schema_name": self.schema_name, "table_name": self.table_name}
            ).scalar()
            return result
        except SQLAlchemyError as e:
            self.logger.error(f"Error checking table existence: {e}")
            raise
            
    def load_full_refresh(self, df: pd.DataFrame):
        """
        Load data using full refresh strategy (truncate and reload).
        
        Args:
            df: DataFrame to load
        """
        self.logger.info(f"Starting full refresh load for {self.full_table_name}")
        
        try:
            # Create schema if not exists
            self._create_schema_if_not_exists()
            
            # Check if table exists, create if not
            if not self._table_exists():
                self._create_table_from_dataframe(df)
            else:
                # Truncate existing table
                self.connection.execute(text(f"TRUNCATE TABLE {self.full_table_name}"))
                self.logger.info(f"Table {self.full_table_name} truncated")
                
            # Insert new data
            df.to_sql(
                name=self.table_name,
                con=self.connection,
                schema=self.schema_name,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            self.connection.commit()
            self.logger.info(f"Successfully loaded {len(df)} rows to {self.full_table_name}")
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error in full refresh load: {e}")
            raise
            
    def load_incremental(
        self,
        df: pd.DataFrame,
        unique_keys: typing.List[str] = None,
        incremental_column: str = None,
        incremental_value: typing.Any = None
    ):
        """
        Load data using incremental strategy (upsert based on unique keys).
        
        Args:
            df: DataFrame to load
            unique_keys: List of columns that form unique key for upsert
            incremental_column: Column used for incremental filtering
            incremental_value: Value for incremental filtering
        """
        self.logger.info(f"Starting incremental load for {self.full_table_name}")
        
        if not unique_keys:
            self.logger.warning("No unique keys provided, falling back to full refresh")
            self.load_full_refresh(df)
            return
            
        try:
            # Create schema if not exists
            self._create_schema_if_not_exists()
            
            # Check if table exists, create if not
            if not self._table_exists():
                self._create_table_from_dataframe(df)
                # If table didn't exist, just insert all data
                df.to_sql(
                    name=self.table_name,
                    con=self.connection,
                    schema=self.schema_name,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                self.connection.commit()
                self.logger.info(f"New table created and loaded {len(df)} rows")
                return
                
            # Perform upsert using PostgreSQL's ON CONFLICT
            self._upsert_data(df, unique_keys)
            
            self.connection.commit()
            self.logger.info(f"Successfully upserted {len(df)} rows to {self.full_table_name}")
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error in incremental load: {e}")
            raise
            
    def _upsert_data(self, df: pd.DataFrame, unique_keys: typing.List[str]):
        """
        Perform upsert operation using PostgreSQL's INSERT ... ON CONFLICT.
        
        Args:
            df: DataFrame to upsert
            unique_keys: List of columns that form unique key
        """
        try:
            # Create temporary table
            temp_table_name = f"temp_{self.table_name}_{int(datetime.now().timestamp())}"
            
            # Load data to temporary table
            df.to_sql(
                name=temp_table_name,
                con=self.connection,
                schema=self.schema_name,
                if_exists='replace',
                index=False,
                method='multi'
            )
            
            # Get column names
            columns = df.columns.tolist()
            columns_str = ", ".join([f'"{col}"' for col in columns])
            
            # Create SET clause for UPDATE
            update_set_clause = ", ".join([
                f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in unique_keys
            ])
            
            # Create unique key constraint clause
            unique_constraint = ", ".join([f'"{key}"' for key in unique_keys])
            
            # Perform upsert
            upsert_query = f"""
                INSERT INTO {self.full_table_name} ({columns_str})
                SELECT {columns_str} FROM {self.schema_name}.{temp_table_name}
                ON CONFLICT ({unique_constraint}) 
                DO UPDATE SET {update_set_clause}
            """
            
            self.connection.execute(text(upsert_query))
            
            # Drop temporary table
            self.connection.execute(text(f"DROP TABLE {self.schema_name}.{temp_table_name}"))
            
            self.logger.info("Upsert operation completed successfully")
            
        except SQLAlchemyError as e:
            self.logger.error(f"Error in upsert operation: {e}")
            raise
            
    def delete_by_condition(self, condition: str, parameters: typing.Dict = None):
        """
        Delete records based on condition.
        
        Args:
            condition: WHERE condition string
            parameters: Parameters for the condition
        """
        try:
            delete_query = f"DELETE FROM {self.full_table_name} WHERE {condition}"
            
            if parameters:
                self.connection.execute(text(delete_query), parameters)
            else:
                self.connection.execute(text(delete_query))
                
            self.connection.commit()
            self.logger.info(f"Delete operation completed for condition: {condition}")
            
        except SQLAlchemyError as e:
            self.connection.rollback()
            self.logger.error(f"Error in delete operation: {e}")
            raise
            
    def get_max_value(self, column_name: str) -> typing.Any:
        """
        Get maximum value from a specific column.
        
        Args:
            column_name: Column to get max value from
            
        Returns:
            Maximum value from the column
        """
        try:
            if not self._table_exists():
                return None
                
            query = text(f'SELECT MAX("{column_name}") FROM {self.full_table_name}')
            result = self.connection.execute(query).scalar()
            
            self.logger.info(f"Max value for {column_name}: {result}")
            return result
            
        except SQLAlchemyError as e:
            self.logger.error(f"Error getting max value: {e}")
            raise
            
    def get_record_count(self) -> int:
        """
        Get total record count in the table.
        
        Returns:
            Number of records in the table
        """
        try:
            if not self._table_exists():
                return 0
                
            query = text(f"SELECT COUNT(*) FROM {self.full_table_name}")
            result = self.connection.execute(query).scalar()
            
            self.logger.info(f"Record count: {result}")
            return result
            
        except SQLAlchemyError as e:
            self.logger.error(f"Error getting record count: {e}")
            raise