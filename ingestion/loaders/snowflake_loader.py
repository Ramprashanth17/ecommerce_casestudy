from ingestion.config.settings import settings
import logging
import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas


# Configure the logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("SnowflakeClient")

class SnowflakeClient:
    """Utility class for Snowflake operations"""
    def __init__(self):
        self.config = settings.snowflake
        self.conn = None

    def _get_connection(self):
        """Creates and returns a Snowflake connection"""
        if self.conn is None or self.conn.is_closed:
            logger.info("Opening new Snowflake connection...")
            self.conn = connect(
                account = self.config.account,
                user = self.config.user,
                password=self.config.password,
                warehouse=self.config.warehouse,
                database=self.config.database,
                role=self.config.role
            )
        return self.conn

    def load_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = None, overwrite : bool =False):
        """Uploads a dataframe to Snowflake.
        Args:
            df: The dataframe to upload
            table_name: Target table name
            schema: Target schema
        """
        target_schema = schema or self.config.schema

        # Get (or reuse) the connection
        connection = self._get_connection()

        success, nchunks, nrows, _ = write_pandas(
            conn=connection,
            df=df,
            table_name=table_name.upper(),
            schema=target_schema.upper(),
            auto_create_table=True,
            
        )
        
        if success:
            logger.info(f"Uploaded {nrows} rows to {table_name}.")
        else:
            raise Exception(f"Failed to upload data to {table_name}")
    
    def close_connection(self):
        """Ensures proper closing of connection"""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed.")
