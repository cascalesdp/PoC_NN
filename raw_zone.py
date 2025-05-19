from config import RAW_CATALOG,RAW_SCHEMA,RAW_VOLUME
from pyspark.sql.types import StructType
import json, datetime

def create_raw_zone_structure(spark):

    catalog_query = f"CREATE CATALOG IF NOT EXISTS {RAW_CATALOG}"
    schema_query = f"CREATE SCHEMA IF NOT EXISTS {RAW_CATALOG}.{RAW_SCHEMA}"
    volume_query = f"CREATE VOLUME IF NOT EXISTS {RAW_CATALOG}.{RAW_SCHEMA}.{RAW_VOLUME}"

    spark.sql(catalog_query)
    spark.sql(schema_query)
    spark.sql(volume_query)

def write_raw_data(spark,row):

    run_date = datetime.date.today().isoformat()

    if row.source_type == "file":

        options = json.loads(row.options_json or "{}")
        schema  = (StructType.fromJson(json.loads(row.schema_json)) if row.schema_json else None)

        data = spark.read.options(**options).schema(schema).format(row.data_format).load(f'{row.connection}{row.source_name}.{row.data_format}')
        
        write_format = row.data_format
            
    elif row.source_type == "jdbc":
        data = spark.read.format(row.source_type).option("url", row.connection).option("dbtable", row.table).load()

        write_format='parquet'
    
    dest = f"{row.raw_target_path}{row.source_name}/ingest_date={run_date}"

    data.write.mode("append").format(write_format).save(dest)