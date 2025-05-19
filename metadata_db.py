from config import METADATA_CATALOG, METADATA_SCHEMA, METADATA_TABLE
import yaml
from pathlib import Path


def create_metadata_database(spark):

    catalog_query = f"CREATE CATALOG IF NOT EXISTS {METADATA_CATALOG}"
    schema_query = f"CREATE SCHEMA IF NOT EXISTS {METADATA_CATALOG}.{METADATA_SCHEMA}"

    spark.sql(catalog_query)
    spark.sql(schema_query)

def create_metadata_table(spark):

    table_query = f"""CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.{METADATA_SCHEMA}.{METADATA_TABLE} (
                        source_id           BIGINT  GENERATED ALWAYS AS IDENTITY,
                        source_name         STRING  NOT NULL,
                        source_type         STRING  NOT NULL,
                        data_format         STRING,      
                        options_json        STRING,
                        connection          STRING,
                        table               STRING,
                        raw_target_path     STRING,
                        hub_target_table    STRING,
                        load_mode           STRING  DEFAULT 'full',
                        incremental_column  STRING,
                        enabled             BOOLEAN DEFAULT true,
                        schema_json         STRING,
                        data_owner          STRING
                        ) USING DELTA
                        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');
                    """
    
    spark.sql(table_query)

def insert_metadata_entries(spark):

    truncate_query = f"""TRUNCATE TABLE {METADATA_CATALOG}.{METADATA_SCHEMA}.{METADATA_TABLE}"""
    spark.sql(truncate_query)

    sources = yaml.safe_load(Path("resources/sources.yaml").read_text(encoding="utf-8"))

    for source in sources.get("sources", []):
        columns = ",".join(source.keys())
        values=[]

        for k in source.keys():
            if isinstance(source[k], bool):
                values.append(f"{source[k]}")
            else:
                values.append(f"'{source[k]}'")
        
        values = ",".join(values)

        source_query = f"""
            INSERT INTO {METADATA_CATALOG}.{METADATA_SCHEMA}.{METADATA_TABLE} ({columns})
            VALUES ({values})
        """
        
        spark.sql(source_query)

def get_metadata_sources(spark):
    return spark.read.table(f"{METADATA_CATALOG}.{METADATA_SCHEMA}.{METADATA_TABLE} ").filter("enabled = True")