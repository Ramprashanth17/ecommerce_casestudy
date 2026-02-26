import logging 
import pandas as pd
from pathlib import Path
from ingestion.config.settings import settings
from ingestion.loaders.snowflake_loader import SnowflakeClient

# Setting up the logging
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

)

logger = logging.getLogger("CSV_Ingestion_Script")

def clean_table_name(filename: str) -> str:
    """ Gets the table name from the file name"""
    name = Path(filename).stem.lower()
    name = name.replace("olist_", "").replace("_dataset","").replace("_data","")
    return name.upper()

def main():
    # Initializing the client
    client = SnowflakeClient()

    # Get all the files 
    raw_data_path = settings.paths.raw_data
    csv_files = list(raw_data_path.glob("*.csv"))

    if not csv_files:
        logger.warning(f"No CSV files found in {raw_data_path}")
        return
    
    logger.info(f"Found {len(csv_files)} files to process.")

    # Loop through the files
    for file_path in csv_files:
        table_name = clean_table_name(file_path.name)

        try:
            logger.info(f"---Processing {file_path.name} -> {table_name} ---")
            df = pd.read_csv(file_path)

            # Load into snowflake
            client.load_dataframe(df, table_name=table_name, overwrite=True)
            logger.info(f"Successfully ingested {table_name}")
        
        except Exception as e:
            logger.error(f"Failed to ingest {file_path.name}: {e}")
            continue

    # close the connection
    client.close_connection()
    logger.info("Ingestion process completed.")



if __name__ == "__main__":
    main()