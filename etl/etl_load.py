import logging
import pandas as pd
import pyspark as ps

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union, Sequence, Optional, Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, col, dayofmonth, weekofyear, month, year, dayofweek, monotonically_increasing_id, upper
)

log = logging.getLogger("etl_load")
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)

# UDF for numeric to datetime conversion; based on SAS start year 1960-1-1
as_datetime = udf(lambda dt: (datetime(1960, 1, 1).date() + timedelta(dt)).isoformat() if dt else None)


class StarSchemaTableError(Exception):
    pass


class StarSchemaTable(ABC):
    """ Abstract base class for any table within resulting star schema. """

    name = ""

    _parquet_name = "default"

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._df_output: Optional[Union[pd.DataFrame, ps.sql.DataFrame]] = None

    @property
    def df(self) -> Union[pd.DataFrame, ps.sql.DataFrame]:
        """
        Returns the output dataframe which should be initialized after calling the setup method.

        Returns:
            Union[pd.DataFrame, ps.sql.DataFrame]: output dataframe
        """

        if not self._df_output:
            log.warning("Table (DataFrame) not initialized - call setup function first")

        return self._df_output

    @abstractmethod
    def setup(self, *args, **kwargs) -> 'StarSchemaTable':
        """ Setup method which needs to be implemented for a specific table class. """

        pass

    def write_parquet(
            self,
            parent_path: Union[str, Path],
            name: str = "",
            partition_cols: Sequence[str] = ()
    ) -> 'StarSchemaTable':
        """
        Writes content of initialized dataframe into parquet format.

        Args:
            parent_path: path of parquet output
            name: name of parquet root folder
            partition_cols: partitioning columns

        Returns:
            StarSchemaTable: current instance of parent class (allows chaining)
        """

        if not self._df_output:
            raise StarSchemaTableError("Table (DataFrame) not initialized - call setup function first")

        p_path = Path(parent_path) if not isinstance(parent_path, Path) else parent_path
        p_path.mkdir(parents=True, exist_ok=True)

        self._df_output.write.parquet(
            str(p_path / (name if name else self._parquet_name)),
            partitionBy=partition_cols,
            mode="overwrite"
        )

        return self

    def verify_data(self, min_rows: int = 10) -> 'StarSchemaTable':
        """ Performs some simple verification (>= 10 entries within loaded data).

        Args:
             min_rows: minimum number of rows within dataframe
        """

        count = self._df_output.count()

        if count < min_rows:
            log.warning(f"The data contains a suspiciously low number of records ({count})")

        return self


class StarSchemaDimTable(StarSchemaTable):
    """ Base class for any dimension table. """

    _primary_key: str = ""

    @abstractmethod
    def setup(self, rename_pk: str) -> 'StarSchemaTable':
        pass


class DimTableImmigrationDate(StarSchemaDimTable):
    name = "DimTableImmigrationDate"

    _parquet_name = "immigration_date"

    _date_col = "arrdate"
    _add_cols = {
        "day": dayofmonth,
        "week": weekofyear,
        "month": month,
        "year": year,
        "weekday": dayofweek
    }

    _primary_key = "arrdate"

    def __init__(self, df_imgn: Union[pd.DataFrame, ps.sql.DataFrame], **kwargs):
        super().__init__(**kwargs)
        self._df_imgn = df_imgn

    def setup(self, rename_pk: str) -> 'StarSchemaDimTable':
        """
        Setup up this table (i.e. fill dataframe and return instance)

        Args:
            rename_pk: rename primary key of this table to given string

        Returns:
            StarSchemaDimTable: current instance of parent class (allows chaining)
        """

        df_date = self._df_imgn \
            .select(self._date_col) \
            .withColumn(self._date_col, as_datetime(col(self._date_col))) \
            .distinct()

        df_date = df_date.withColumn("id", monotonically_increasing_id())

        for col_name, date_function in self._add_cols.items():
            df_date = df_date.withColumn(col_name, date_function(col(self._date_col)))

        self._df_output = df_date.withColumnRenamed(self._primary_key, rename_pk)

        return self


class DimTableCountry(StarSchemaDimTable):
    name = "DimTableCountry"

    _parquet_name = "country"

    _col_country = "country"
    _col_country_code = "country_code"
    _col_temp_avg = "temperature_avg"

    _primary_key = "country_code"

    def __init__(
            self,
            df_imgn: Union[pd.DataFrame, ps.sql.DataFrame],
            df_code: Union[pd.DataFrame, ps.sql.DataFrame],
            df_temp: Union[pd.DataFrame, ps.sql.DataFrame],
            **kwargs
    ):
        super().__init__(**kwargs)
        self._df_imgn = df_imgn
        self._df_code = df_code
        self._df_temp = df_temp

    def setup(self, rename_pk: str) -> 'StarSchemaDimTable':
        """
        Setup up this table (i.e. fill dataframe and return instance)

        Args:
            rename_pk: rename primary key of this table to given string

        Returns:
            StarSchemaDimTable: current instance of parent class (allows chaining)
        """

        df_country = self._df_imgn \
            .join(self._df_code, self._df_code["code"] == self._df_imgn["i94res"], "left") \
            .select(["i94res", self._col_country]) \
            .withColumnRenamed("i94res", self._col_country_code) \
            .distinct()

        df_temp_country = self._df_temp \
            .select(["Country", "AverageTemperature"]) \
            .groupby("Country") \
            .avg() \
            .withColumnRenamed("avg(AverageTemperature)", self._col_temp_avg) \
            .withColumnRenamed("Country", "temp_country")

        df_country = df_country \
            .join(df_temp_country, upper(df_temp_country["temp_country"]) == df_country[self._col_country]) \
            .select([self._col_country_code, self._col_country, self._col_temp_avg]) \
            .distinct()

        self._df_output = df_country.withColumnRenamed(self._primary_key, rename_pk)

        return self


class DimTableAirport(StarSchemaDimTable):
    name = "DimTableAirport"

    _parquet_name = "airport"

    _primary_key = "port"

    def __init__(
            self,
            df_imgn: Union[pd.DataFrame, ps.sql.DataFrame],
            df_airp: Union[pd.DataFrame, ps.sql.DataFrame],
            **kwargs
    ):
        super().__init__(**kwargs)
        self._df_imgn = df_imgn
        self._df_airp = df_airp

    def setup(self, rename_pk: str) -> 'StarSchemaDimTable':
        """
        Setup up this table (i.e. fill dataframe and return instance)

        Args:
            rename_pk: rename primary key of this table to given string

        Returns:
            StarSchemaDimTable: current instance of parent class (allows chaining)
        """

        self._df_imgn.createOrReplaceTempView("immigration")
        self._df_airp.createOrReplaceTempView("airport")

        df_iata = self._spark.sql(
            """
            SELECT
                DISTINCT im.i94port AS {primary_key},
                ap.*
            FROM
                immigration AS im
            LEFT JOIN
                airport AS ap ON (im.i94port = ap.iata_code AND im.i94mode = 1)
            """.format(primary_key=self._primary_key)
        )

        self._df_output = df_iata.withColumnRenamed(self._primary_key, rename_pk)

        return self


class DimTableDemo(StarSchemaDimTable):
    name = "DimTableDemo"

    _parquet_name = "demographics"

    _agg_cols = ['Median Age', 'Male Population', 'Female Population', 'Total Population',
                 'Number of Veterans', 'Foreign-born', 'Average Household Size']
    _group_cols = ['State Code', 'State', 'City']

    _primary_key = "State Code"

    def __init__(
            self,
            df_imgn: Union[pd.DataFrame, ps.sql.DataFrame],
            df_demo: Union[pd.DataFrame, ps.sql.DataFrame],
            **kwargs
    ):
        super().__init__(**kwargs)
        self._df_imgn = df_imgn
        self._df_demo = df_demo

    def setup(self, rename_pk: str) -> 'StarSchemaDimTable':
        """
        Setup up this table (i.e. fill dataframe and return instance)

        Args:
            rename_pk: rename primary key of this table to given string

        Returns:
            StarSchemaDimTable: current instance of parent class (allows chaining)
        """

        fix_s = lambda string: string.replace(' ', '')

        self._df_demo.createOrReplaceTempView("demographics")

        # The demographic data is transformed such that the resulting table contains
        # columns for each race count. Also, the data is grouped by city, state and state code,
        # and the aggregated data corresponds to averages.
        df_demo = self._spark.sql(
            """
            SELECT
                {group_cols}, {avg_agg_cols}, 
                avg(RaceCountHispLat) AS RaceCountHispLatAvg, 
                avg(RaceCountWhite) AS RaceCountWhiteAvg,
                avg(RaceCountAsian) AS RaceCountAsianAvg,
                avg(RaceCountBlackAfr) AS RaceCountBlackAfrAvg,
                avg(RaceCountNative) AS RaceCountNativeAvg
            FROM
                (
                    SELECT 
                        {group_cols}, {agg_cols}, 
                        CASE
                            WHEN Race = 'Hispanic or Latino' THEN Count
                            ELSE 0
                        END AS RaceCountHispLat,
                        CASE
                            WHEN Race = 'White' THEN Count
                            ELSE 0
                        END AS RaceCountWhite,
                        CASE
                            WHEN Race = 'Asian' THEN Count
                            ELSE 0
                        END AS RaceCountAsian,
                        CASE
                            WHEN Race = 'Black or African-American' THEN Count
                            ELSE 0
                        END AS RaceCountBlackAfr,
                        CASE
                            WHEN Race = 'American Indian and Alaska Native' THEN Count
                            ELSE 0
                        END AS RaceCountNative
                    FROM
                        demographics
                ) AS sub 
            GROUP BY
                 {group_cols}
            """.format(
                group_cols=", ".join([f"`{c}`" for c in self._group_cols]),
                agg_cols=", ".join([f"`{c}`" for c in self._agg_cols]),
                avg_agg_cols=", ".join([f"avg(`{c}`) AS `{fix_s(c)}Avg`" for c in self._agg_cols])
            )
        )

        self._df_imgn.createOrReplaceTempView("immigration")
        df_demo.createOrReplaceTempView("demo_aggregated")

        # Make sure that all primary keys correspond to foreign keys in fact table
        self._df_output = self._spark.sql(
            """
            SELECT
                im.i94addr AS new_pk,
                dmg.* 
            FROM
                immigration AS im
            LEFT JOIN
                demo_aggregated AS dmg ON (dmg.`{primary_key}` = im.i94addr)
            """.format(primary_key=self._primary_key)
        )

        self._df_output = self._df_output \
            .drop(self._primary_key) \
            .withColumnRenamed("new_pk", rename_pk) \
            .dropna(subset=[rename_pk]) \
            .groupby([rename_pk, "State"]).avg()

        for c in self._df_output.columns:
            if c.startswith("avg(") and c.endswith(")"):
                self._df_output = self._df_output.withColumnRenamed(c, c[4:-1])

        return self


class FactTableImmigration(StarSchemaTable):
    """ Specific class for the fact table. """

    name = "FactTableImmigration"

    _parquet_name = "immigration"

    def __init__(self, df_imgn: Union[pd.DataFrame, ps.sql.DataFrame], **kwargs):
        super().__init__(**kwargs)
        self._df_imgn = df_imgn

    def setup(self, rename_dim_pks: Dict[str, str], date_cols: Sequence[str]) -> 'FactTableImmigration':
        """
        Set up the immigration fact table.

        Args:
            rename_dim_pks: rename the composite keys from the dimension tables; NOTE: the naming must be
                consistent with the chosen primary keys of the dimension tables.
            date_cols: type-casts provided date columns into correct date-type

        Returns:
            FactTableImmigration: current instance of parent class (allows chaining)
        """

        df_imgn = self._df_imgn
        for date_col in date_cols:
            df_imgn = df_imgn.withColumn(date_col, as_datetime(col(date_col)))

        for from_key, to_key in rename_dim_pks.items():
            df_imgn = df_imgn.withColumnRenamed(from_key, to_key)

        self._df_output = df_imgn

        return self
