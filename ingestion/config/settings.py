import os
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv

# 1. Load the .env file 
load_dotenv()

@dataclass(frozen=True)     #Decorator used to add generated special methods to classes, fronze = True, makes setting immutable
class SnowflakeConfig:
    """Snowflake connection parameters"""
    account: str = os.getenv("SNOWFLAKE_ACCOUNT")
    user: str = os.getenv("SNOWFLAKE_USER")
    password: str = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE")
    database: str = os.getenv("SNOWFLAKE_DATABASE")
    role: str = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    schema: str = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

@dataclass(frozen=True)
class FilePathConfig:
    """System paths for data ingestion"""
    project_root: Path = Path(__file__).parent.parent.parent
    raw_data_dir: str = "kaggle_csv_files" 

    @property
    def raw_data(self) -> Path:
        return self.project_root / self.raw_data_dir
    
@dataclass(frozen= True)
class APIConfig:
    """External API keys and endpoints"""
    #frankfurter_api_key: str = os.getenv("")
    frankfurter_base_url: str = "https://api.frankfurter.dev/v1"
    open_weather_api_key: str = os.getenv("OPENWEATHER_API_KEY", "")
    ibge_base_url: str = "https://servicodados.ibge.gov.br/api"
    #ibge_api_key: str = os.getenv("")

@dataclass(frozen= True)
class Settings:
    """Main Settings object"""
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig)
    paths: FilePathConfig = field(default_factory= FilePathConfig)
    api: APIConfig = field(default_factory= APIConfig)
    env: str = os.getenv("ENV", "dev")


# Instance to be imported elsewhere
settings = Settings()

# if __name__ == "__main__":
#     print(f"Project root: {settings.paths.project_root}")
#     print(f"Raw data path: {settings.paths.raw_data}")
#     print(f"Path exists: {settings.paths.raw_data.exists()}")