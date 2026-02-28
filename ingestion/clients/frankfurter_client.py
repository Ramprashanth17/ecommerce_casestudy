from ingestion.config.settings import settings
from ingestion.loaders.snowflake_loader import SnowflakeClient
from datetime import timedelta, datetime, date
import pandas as pd
import requests
import logging

logger = logging.getLogger("Frankfurter_Ingestion")

class FrankfurterIngestor:
    def __init__(self):
        self.base_url = settings.api.frankfurter_base_url
        self.client = SnowflakeClient()

    def fetch_exchange_rates(self, start_date, end_date, base='BRL', to='USD'):
        # Calling the URL
        url = f"{self.base_url}/{start_date}..{end_date}"
        params = {"base":base, "to": to}

        try:
            logger.info(f"Fetching {base} to {to} from {start_date} to {end_date}")
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            rates_dict = data.get('rates', {})
            df = pd.DataFrame.from_dict(rates_dict, orient='index')
            df = df.reset_index()

            df.columns = ['EXCHANGE_DATE', 'USD_RATE']
            #df['EXCHANGE_DATE'] = pd.to_datetime(df['EXCHANGE_DATE'], unit='ns')
            df['EXCHANGE_DATE'] = df['EXCHANGE_DATE'].dt.date

            return df

        except Exception as e:
            logger.error(f"API request failed. {e}")
            raise
    
    def run_incremental(self):
        cursor = self.client._get_connection().cursor()
        try:
            cursor.execute("SELECT MAX(EXCHANGE_DATE) FROM RAW.CURRENCY_RATES")
            result = cursor.fetchone()
            if result[0] is None:
                logger.info("Table is empty. Starting from default date")
                next_start_date = date(2016, 9, 1)
            else:
                next_start_date = result[0] + timedelta(days=1)
            
            end_date = date.today()

            if next_start_date < end_date:
                logger.info(f"Incremental run: {next_start_date} to {end_date}")
                df = self.fetch_exchange_rates(start_date= next_start_date.isoformat(),
                                               end_date=end_date.isoformat())
                
                if not df.empty:
                    self.client.load_dataframe(df, table_name="CURRENCY_RATES", overwrite=False)
            
            else:
                logger.info("Data is already upto date.")

        finally:
            cursor.close()
            self.client.close_connection()
        
        




    
    def run(self, start_date: str, end_date: str):
        try:
            df = self.fetch_exchange_rates(start_date, end_date)
            if not df.empty:
                self.client.load_dataframe(df, table_name="CURRENCY_RATES", overwrite=False)
                logger.info(f"Successfully loaded {len(df)} rows to Snowflake")
            else:
                logger.warning("No data found for the given date range")
        except Exception as e:
            logger.critical(f"Ingestion Pipeline failed: {e}")
        finally:
            self.client.close_connection()

def main():
    ingestor = FrankfurterIngestor()
    ingestor.run_incremental()
    #ingestor.run(start_date='2016-09-01', end_date='2018-10-31')

if __name__ == "__main__":
    main()


