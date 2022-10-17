import logging
import os

import pandas as pd
import pyspark as ps
import argparse as ap

from dataclasses import dataclass
from pathlib import Path
from typing import Union
from pyspark.sql import SparkSession

from config import (
    LoadDataframesConfig,
    StarSchemaConfig,
    ETLConfig,
    SchemaToParquetConfig,
    write_config_to_json,
    load_config_from_json
)
from etl_load import (
    DimTableImmigrationDate,
    DimTableCountry,
    DimTableAirport,
    DimTableDemo,
    FactTableImmigration,
    StarSchemaTable
)
from etl_extract import SourceLoader

log = logging.getLogger("ETL")
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)


@dataclass
class LoadDataContainerDataframe:
    """ Container class that stores dataframes of import data. """

    df_imgn: Union[pd.DataFrame, ps.sql.DataFrame]
    df_demo: Union[pd.DataFrame, ps.sql.DataFrame]
    df_temp: Union[pd.DataFrame, ps.sql.DataFrame]
    df_airp: Union[pd.DataFrame, ps.sql.DataFrame]
    df_codes: Union[pd.DataFrame, ps.sql.DataFrame]


@dataclass
class StarSchemaContainerSpark:
    """ Container class that stores (dimension/fact) tables within resulting star schema. """

    dim_date: StarSchemaTable
    dim_country: StarSchemaTable
    dim_airport: StarSchemaTable
    dim_demo: StarSchemaTable
    fact_immigration: StarSchemaTable


def setup_env(
        config: ETLConfig,
        set_aws: bool = False
):
    """
    Sets up environment variables, e.g. for AWS.

    Args:
        config: configuration for this ETL run
        set_aws: flag to initialize AWS

    """

    log.info("Initializing environment")

    if set_aws and config.aws_access_key_id and config.aws_secret_access_key:
        os.environ["AWS_ACCESS_KEY_ID"] = config.aws_access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = config.aws_secret_access_key


def initialize_spark_session() -> SparkSession:
    """ Initialize a spark session with the required defaults. """

    log.info("Initializing Spark session")

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("spark.driver.memory", "15g") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


def load_into_dataframes(
        spark: SparkSession,
        config: LoadDataframesConfig = LoadDataframesConfig(),
        mode: str = "spark"
) -> LoadDataContainerDataframe:
    """
    Load data from source into corresponding dataframe instances.

    NOTE: this method could e.g. be improved by providing a config-class for the used options.

    Args:
        spark: initialized spark session
        config: configuration class
        mode: 'spark' or 'pandas' (default: 'spark') - only supports 'spark' at the moment for further processing

    Returns:
        LoadDataContainerDataframe: container that keeps output dataframes
    """

    log.info(f"Loading source data into dataframes (mode: {mode})")

    df_imgn = SourceLoader(source_path=config.immigration.source_path, mode=mode, spark_session=spark) \
        .load() \
        .clean(to_type_cols=config.immigration.to_type_cols, drop_cols=config.immigration.drop_cols) \
        .verify_data() \
        .df

    df_demo = SourceLoader(source_path=config.demographic.source_path, mode=mode, spark_session=spark) \
        .load(csv_sep=config.demographic.csv_sep) \
        .clean(dropna_cols=config.demographic.dropna_cols) \
        .verify_data() \
        .df

    df_temp = SourceLoader(source_path=config.temperature.source_path, mode=mode, spark_session=spark) \
        .load() \
        .clean(dropna_cols=config.temperature.dropna_cols, drop_duplicate_cols=config.temperature.drop_duplicate_cols) \
        .verify_data() \
        .df

    df_airp = SourceLoader(source_path=config.airport.source_path, mode=mode, spark_session=spark) \
        .load(csv_sep=config.airport.csv_sep) \
        .clean(drop_duplicate_cols=config.airport.drop_duplicate_cols) \
        .verify_data() \
        .df

    df_codes = SourceLoader(source_path=config.codes.source_path, mode=mode, spark_session=spark) \
        .load(csv_sep=config.codes.csv_sep) \
        .clean(to_type_cols=config.codes.to_type_cols) \
        .verify_data() \
        .df

    return LoadDataContainerDataframe(df_imgn, df_demo, df_temp, df_airp, df_codes)


def create_star_schema_spark(
        spark: SparkSession,
        df_imgn: Union[pd.DataFrame, ps.sql.DataFrame],
        df_demo: Union[pd.DataFrame, ps.sql.DataFrame],
        df_temp: Union[pd.DataFrame, ps.sql.DataFrame],
        df_airp: Union[pd.DataFrame, ps.sql.DataFrame],
        df_code: Union[pd.DataFrame, ps.sql.DataFrame],
        config: StarSchemaConfig = StarSchemaConfig()
) -> StarSchemaContainerSpark:
    """
    Based on input data within spark/pandas dataframes, returns a container incorporating all tables
    of the resulting star schema as dataframes.

    NOTE: this method could e.g. be improved by providing a config-class for the used options.

    Args:
        spark: initialized spark session
        df_imgn: dataframe of immigration data
        df_demo: dataframe of demographic data
        df_temp: dataframe of temperature data
        df_airp: dataframe of airports data
        df_code: dataframe of SAS mappings for country codes
        config: configuration for initialization of star schema

    Returns:
        StarSchemaContainerSpark: container of resulting (table) dataframes
    """

    log.info("Setting up star schema member dataframes")

    dim_date = DimTableImmigrationDate(df_imgn, spark=spark) \
        .setup(rename_pk=config.dim_date_pk) \
        .verify_data()

    dim_country = DimTableCountry(df_imgn, df_code, df_temp, spark=spark) \
        .setup(rename_pk=config.dim_country_pk) \
        .verify_data()

    dim_airport = DimTableAirport(df_imgn, df_airp, spark=spark) \
        .setup(rename_pk=config.dim_airport_pk) \
        .verify_data()

    dim_demo = DimTableDemo(df_imgn, df_demo, spark=spark) \
        .setup(rename_pk=config.dim_demo_pk) \
        .verify_data()

    fact_immigration = FactTableImmigration(df_imgn, spark=spark) \
        .setup(rename_dim_pks=config.fact_immigration.rename_dim_pks, date_cols=config.fact_immigration.date_cols) \
        .verify_data()

    container = StarSchemaContainerSpark(dim_date, dim_country, dim_airport, dim_demo, fact_immigration)

    return container


def write_schema_parquet(
        ct_star: StarSchemaContainerSpark,
        config: SchemaToParquetConfig
):
    """
    Write schema member tables to parquet.

    Args:
        ct_star: container of schema dataframes
        config: configuration object for output

    """

    log.info("Performing parquet output of star schema member tables")

    c = config

    ct_star.dim_date.write_parquet(c.dim_date.path, c.dim_date.name, c.dim_date.partition_cols)
    ct_star.dim_country.write_parquet(c.dim_country.path, c.dim_country.name)
    ct_star.dim_airport.write_parquet(c.dim_airport.path, c.dim_airport.name)
    ct_star.dim_demo.write_parquet(c.dim_demographic.path, c.dim_demographic.name)
    ct_star.fact_immigration.write_parquet(c.fact_immigration.path, c.fact_immigration.name,
                                           c.fact_immigration.partition_cols)


def print_schema_info(
        ct_star: StarSchemaContainerSpark
):
    for t in [ct_star.dim_date, ct_star.dim_country, ct_star.dim_airport, ct_star.dim_demo, ct_star.fact_immigration]:
        log.info(f"Info for table object: {t.name}")
        t.df.printSchema()


def main(
        write_parquet: Union[str, Path] = "",
        load_config: Union[str, Path] = "",
        write_config: Union[str, Path] = "",
        get_schema_info: bool = False
):
    """
    Main method performing the ETL process.

    Args:
        write_parquet: path to parquet output (default: "" - produces no output)
        load_config: path to configuration JSON file (default: "" - uses default ETLConfig if not provided)
        write_config: write config to path (default: "" - don't write)
        get_schema_info: print table schema info to console
    """

    # Load or setup default ETL config
    config = ETLConfig() if not load_config else load_config_from_json(Path(load_config))

    # Write ETL config to JSON file, if requested
    if write_config:
        config_path = Path(write_config) if not isinstance(write_config, Path) else write_config
        json_path = config_path / "./config.json"

        log.info(f"Dumping default config to JSON: {json_path}")
        write_config_to_json(config, json_path)

        return 0

    # Possibly set up environment (e.g. for AWS)
    setup_env(config)

    log.info("Starting ETL process")

    # Set up spark session object
    spark = initialize_spark_session()

    # Load source data into spark dataframes
    ct_load = load_into_dataframes(spark, config.load_config)

    # Use source dataframes into star schema
    ct_star = create_star_schema_spark(
        spark,
        ct_load.df_imgn,
        ct_load.df_demo,
        ct_load.df_temp,
        ct_load.df_airp,
        ct_load.df_codes,
        config=config.schema_config
    )

    # Write table objects to parquet, if requested
    if write_parquet:
        write_schema_parquet(ct_star, config.parquet_config)

    log.info("Finished ETL process")

    # Write table objects info to console, if requested
    if get_schema_info:
        print_schema_info(ct_star)

    return 0


if __name__ == "__main__":
    parser = ap.ArgumentParser(description="ETL process for immigration data")
    parser.add_argument("-wp", "--write-parquet", action="store_true", dest="write_parquet", default=False,
                        help="Write output to parquet files (see config for paths)")
    parser.add_argument("-lc", "--load-config", action="store", dest="load_config", default="",
                        help="Load config from JSON file in provided path")
    parser.add_argument("-wc", "--write-config", action="store", dest="write_config", default="",
                        help="Write default config to JSON and exit")
    parser.add_argument("-i", "--info-schema", action="store_true", dest="info_schema", default=False,
                        help="Print out table schema information to console")
    results = parser.parse_args()

    main(
        write_parquet=results.write_parquet,
        load_config=results.load_config,
        write_config=results.write_config,
        get_schema_info=results.info_schema
    )
