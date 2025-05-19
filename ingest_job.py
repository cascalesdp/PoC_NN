from utils import get_spark
import metadata_db as md
import raw_zone   as rz
import data_hub   as dh

def main():
    spark = get_spark()

    md.create_metadata_database(spark)
    md.create_metadata_table(spark)
    md.insert_metadata_entries(spark)

    rz.create_raw_zone_structure(spark)
    dh.create_data_hub_structure(spark)

    sources_df = md.get_metadata_sources(spark)

    for row in sources_df.toLocalIterator():
        rz.write_raw_data(spark,row)
        dh.write_data_hub_table(spark,row)

if __name__ == "__main__":
    main()