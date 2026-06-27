import os
import sys

# Simple dotenv parser to load .env variables if present
def load_dotenv(dotenv_path=".env"):
    if os.path.exists(dotenv_path):
        with open(dotenv_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if key not in os.environ:
                        os.environ[key] = val

# Load .env file at startup
load_dotenv()

class Settings:
    def __init__(self):
        self.config_data = {}
        self.config_path = self._find_config_path()
        self.load_yaml()

    def _find_config_path(self):
        # Look in current directory and parent directories
        current = os.getcwd()
        for _ in range(3):
            candidate = os.path.join(current, "config.yaml")
            if os.path.exists(candidate):
                return candidate
            current = os.path.dirname(current)
        return "config.yaml"

    def load_yaml(self):
        if not os.path.exists(self.config_path):
            print(f"Warning: config.yaml not found at {self.config_path}. Using environment defaults.")
            return

        try:
            import yaml
            with open(self.config_path, "r") as f:
                self.config_data = yaml.safe_load(f) or {}
        except ImportError:
            # Simple fallback if PyYAML is not installed yet
            print("Warning: PyYAML not installed. Parsing config.yaml using basic parser.")
            self._parse_yaml_fallback()
        except Exception as e:
            print(f"Error loading config.yaml: {e}")

    def _parse_yaml_fallback(self):
        # Extremely basic YAML parser to handle simple config.yaml structures without dependencies
        current_section = None
        current_sub_section = None
        
        with open(self.config_path, "r") as f:
            for line in f:
                line = line.split("#")[0].strip()
                if not line:
                    continue
                
                # Check for sections
                if line.endswith(":"):
                    name = line[:-1].strip()
                    if not line.startswith(" "):
                        current_section = name
                        self.config_data[current_section] = {}
                        current_sub_section = None
                    else:
                        current_sub_section = name
                        self.config_data[current_section][current_sub_section] = {}
                elif ":" in line:
                    key, val = line.split(":", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    
                    # Convert types
                    if val.lower() == "true":
                        val = True
                    elif val.lower() == "false":
                        val = False
                    elif val.lower() in ("null", "none"):
                        val = None
                    
                    if current_sub_section and current_section:
                        self.config_data[current_section][current_sub_section][key] = val
                    elif current_section:
                        self.config_data[current_section][key] = val
                    else:
                        self.config_data[key] = val

    @property
    def environment(self) -> str:
        # env overrides config
        return os.environ.get("RCA_ENVIRONMENT", self.config_data.get("environment", "local")).lower()

    @property
    def is_local(self) -> bool:
        return self.environment == "local"

    @property
    def is_databricks(self) -> bool:
        return self.environment == "databricks"

    @property
    def logs_base_path(self) -> str:
        # Allow env override
        env_val = os.environ.get("RCA_LOGS_BASE_PATH")
        if env_val:
            return env_val.replace("\\", "/")

        if self.is_local:
            local_cfg = self.config_data.get("local", {})
            return local_cfg.get("logs_dir", "./data/RCA_Logging_Artifacts").replace("\\", "/")
        else:
            db_cfg = self.config_data.get("databricks", {})
            # Try to resolve user email/workspace dynamically if base path contains gandhi...
            base_path = db_cfg.get("logs_base_path", "/tmp/RCA_Logging_Artifacts")
            # If we are in Databricks and can get username/email
            if "gandhi.yatharth@gmail.com" in base_path:
                try:
                    # In Databricks, we can fetch username
                    import sys
                    if "py4j" in sys.modules or "pyspark" in sys.modules:
                        from pyspark.sql import SparkSession
                        spark = SparkSession.getActiveSession()
                        if spark:
                            # Try to get context
                            username = spark.conf.get("spark.databricks.workspaceUrl", None) # or get via dbutils
                            # For safety, keep user configuration or fallback to /tmp
                except Exception:
                    pass
            return base_path.replace("\\", "/")

    @property
    def openrouter_api_key(self) -> str:
        key = os.environ.get("OPENROUTER_API_KEY")
        if not key:
            # Fallback to Databricks secrets
            try:
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                if spark:
                    dbutils = DBUtils(spark)
                    key = dbutils.secrets.get(scope="openrouter", key="api-key")
            except Exception:
                pass
        return key

    @property
    def openrouter_base_url(self) -> str:
        llm_cfg = self.config_data.get("llm", {})
        return llm_cfg.get("base_url", "https://openrouter.ai/api/v1")

    @property
    def llm_model(self) -> str:
        llm_cfg = self.config_data.get("llm", {})
        return llm_cfg.get("model", "poolside/laguna-xs.2:free")

    def get_raw_path(self, dataset_name: str) -> str:
        if self.is_local:
            local_cfg = self.config_data.get("local", {})
            paths = local_cfg.get("raw_paths", {})
            return paths.get(dataset_name, f"./data/source_data/raw/{dataset_name}/*.csv")
        else:
            db_cfg = self.config_data.get("databricks", {})
            raw_base = db_cfg.get("raw_volumes_path", "/Volumes/ecommerce/source_data/raw")
            return f"{raw_base}/{dataset_name}/*.csv"

    def get_table_name(self, table_key: str) -> str:
        if self.is_local:
            local_cfg = self.config_data.get("local", {})
            tables = local_cfg.get("tables", {})
            tbl_name = tables.get(table_key, table_key)
            
            schema = "bronze"
            if "slv_" in table_key:
                schema = "silver"
            elif "gld_" in table_key:
                schema = "gold"
                
            return f"{schema}.{tbl_name}"
        else:
            db_cfg = self.config_data.get("databricks", {})
            catalog = db_cfg.get("catalog", "ecommerce")
            
            # Map key to standard medallion schema
            schema = "bronze"
            if "slv_" in table_key:
                schema = "silver"
            elif "gld_" in table_key:
                schema = "gold"
                
            local_cfg = self.config_data.get("local", {})
            tables = local_cfg.get("tables", {})
            tbl_name = tables.get(table_key, table_key)
            # Remove prefixes
            tbl_name = tbl_name.replace("brz_", "").replace("slv_", "").replace("gld_", "")
            
            return f"{catalog}.{schema}.{tbl_name}"

# Singleton settings instance
settings = Settings()
