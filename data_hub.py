from config import DATAHUB_CATALOG,DATAHUB_SCHEMA
from pyspark.sql.types import StructType
import json, datetime

def create_data_hub_structure(spark):
    
    catalog_query = f"CREATE CATALOG IF NOT EXISTS {DATAHUB_CATALOG}"
    schema_query = f"CREATE SCHEMA IF NOT EXISTS {DATAHUB_CATALOG}.{DATAHUB_SCHEMA}"

    spark.sql(catalog_query)
    spark.sql(schema_query)



def write_data_hub_table(spark,row):

    run_date = datetime.date.today().isoformat()

    options = json.loads(row.options_json or "{}")
    schema  = (StructType.fromJson(json.loads(row.schema_json)) if row.schema_json else None)
    
    if row.source_type == "file":
        df_raw = spark.read.options(**options).schema(schema).format(row.data_format).load(f'{row.raw_target_path}{row.source_name}')

    else:
        df_raw = spark.read.format('parquet').load(f'{row.raw_target_path}{row.source_name}')

    if row.load_mode == "full" or not spark.catalog.tableExists(row.hub_target_table):
        df_raw = df_raw.filter(f"ingest_date = '{run_date}'")
        df_raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("mergeSchema","true").saveAsTable(row.hub_target_table)
    else:
        df_raw.createOrReplaceTempView("tmp_df")
        spark.sql(f"""
          MERGE INTO {row.hub_target_table} AS t
          USING (SELECT * FROM tmp_df) AS s
          ON   t.{row.incremental_column} = s.{row.incremental_column}
          WHEN NOT MATCHED THEN INSERT *                               
        """)