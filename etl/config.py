import json

from pathlib import Path
from typing import Sequence, Dict, Optional, Union
from pydantic import BaseModel, Field


class SourceLoaderConfig(BaseModel):
    source_path: Optional[str] = Field(
        default=None,
        description="Source path"
    )
    csv_sep: str = Field(
        default=";",
        description="CSV separator"
    )
    dropna_cols: Optional[Sequence[str]] = Field(
        default=None,
        description="Columns to drop"
    )
    drop_duplicate_cols: Optional[Sequence[str]] = Field(
        default=None,
        description="Drop duplicates entries via cols"
    )
    drop_cols: Optional[Sequence[str]] = Field(
        default=None,
        description="Drop columns"
    )
    to_type_cols: Optional[Dict[str, str]] = Field(
        default=None,
        description="Transforms columns to provided types (['int', 'float', 'str'])"
    )


class LoadDataframesConfig(BaseModel):
    immigration: SourceLoaderConfig = SourceLoaderConfig(
        source_path="../data/immigration_data_sample.csv",
        to_type_cols={"cicid": "int", "i94yr": "int", "i94mon": "int", "biryear": "int", "arrdate": "int",
                      "i94res": "int"},
        drop_cols=["occup", "entdepu", "insnum"]
    )
    demographic: SourceLoaderConfig = SourceLoaderConfig(
        source_path="../data/us-cities-demographics.csv",
        csv_sep=";",
        dropna_cols=["Male Population", "Female Population", "Number of Veterans", "Foreign-born",
                     "Average Household Size"]
    )
    temperature: SourceLoaderConfig = SourceLoaderConfig(
        source_path="../data/GlobalLandTemperaturesByCity_sample.csv",
        dropna_cols=["AverageTemperature"],
        drop_duplicate_cols=["dt", "City", "Country"]
    )
    airport: SourceLoaderConfig = SourceLoaderConfig(
        source_path="../data/airport-codes_csv.csv",
        csv_sep=",",
        drop_duplicate_cols=["iata_code"]
    )
    codes: SourceLoaderConfig = SourceLoaderConfig(
        source_path="../data/SAS_I94_country_codes.csv",
        csv_sep=";",
        to_type_cols={"code": "int"}
    )


class FactTableConfig(BaseModel):
    rename_dim_pks: Dict[str, str] = Field(
        default={"arrdate": "arrival_date", "i94res": "country_code", "i94port": "arrival_port",
                 "i94addr": "state_code"},
        description="Composite primary key mappings for dimension table keys"
    )
    date_cols: Sequence[str] = Field(
        default=["arrdate", "depdate"],
        description="Columns to be casted to date-type"
    )


class StarSchemaConfig(BaseModel):
    dim_date_pk: str = Field(
        default="arrival_date",
        description="Primary key of date dimension table"
    )
    dim_country_pk: str = Field(
        default="country_code",
        description="Primary key of country dimension table"
    )
    dim_airport_pk: str = Field(
        default="arrival_port",
        description="Primary key of airport dimension table"
    )
    dim_demo_pk: str = Field(
        default="state_code",
        description="Primary key of demographics dimension table"
    )
    fact_immigration: FactTableConfig = Field(
        default=FactTableConfig(),
        description="Fact table configuration for types and composite keys"
    )


class ParquetFileConfig(BaseModel):
    path: str = Field(
        default="../output_data",
        description="Path to write parquet files to"
    )
    name: str = Field(
        default="",
        description="Name of root folder"
    )
    partition_cols: Sequence[str] = Field(
        default=[],
        description="Partition columns"
    )


class SchemaToParquetConfig(BaseModel):
    dim_date: ParquetFileConfig = Field(
        default=ParquetFileConfig(name="DIM_arrival_date", partition_cols=("year", "month")),
        description="Parquet config for date dimension table dataframe"
    )
    dim_country: ParquetFileConfig = Field(
        default=ParquetFileConfig(name="DIM_origin_country"),
        description="Parquet config for country dimension table dataframe"
    )
    dim_airport: ParquetFileConfig = Field(
        default=ParquetFileConfig(name="DIM_arrival_airport"),
        description="Parquet config for airport dimension table dataframe"
    )
    dim_demographic: ParquetFileConfig = Field(
        default=ParquetFileConfig(name="DIM_demographics"),
        description="Parquet config for demographics dimension table dataframe"
    )
    fact_immigration: ParquetFileConfig = Field(
        default=ParquetFileConfig(name="FACT_immigration", partition_cols=("state_code",)),
        description="Parquet config for immigration fact table dataframe"
    )


class ETLConfig(BaseModel):
    aws_access_key_id: str = Field(
        default="",
        description="An AWS access key ID"
    )
    aws_secret_access_key: str = Field(
        default="",
        description="An AWS secret access key"
    )
    load_config: LoadDataframesConfig = Field(
        default=LoadDataframesConfig(),
        description="Load config"
    )
    schema_config: StarSchemaConfig = Field(
        default=StarSchemaConfig(),
        description="Configuration of star schema members"
    )
    parquet_config: SchemaToParquetConfig = Field(
        default=SchemaToParquetConfig(),
        description="Parquet output config"
    )


def write_config_to_json(
        config: ETLConfig,
        path: Union[str, Path]
):
    """
    Write an ETLConfig object to a JSON file.

    Args:
        config: ETLConfig instance
        path: path to write JSON to

    """

    with open(path, "w") as f:
        json.dump(config.dict(), f)


def load_config_from_json(
        path: Union[str, Path]
) -> ETLConfig:
    """
    Load ETLConfig settings from JSON file and return instance.

    Args:
        path: path of JSON file

    Returns:
        ETLConfig: config instance
    """

    with open(path, "r") as f:
        config_dict = json.loads(f.read())
        return ETLConfig(**config_dict)
