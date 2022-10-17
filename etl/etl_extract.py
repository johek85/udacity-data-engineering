import logging
import pandas as pd
import pyspark as ps

from pathlib import Path
from typing import Union, Optional, Sequence, Dict
from pyspark.sql import functions as F

log = logging.getLogger("etl_extract")
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)

FORMAT_CSV = ".csv"
FORMAT_SAS = ".sas7bdat"

TYPE_STR = "str"
TYPE_INT = "int"
TYPE_FLOAT = "float"
TYPE_DATETIME = "datetime"


class SourceLoaderError(Exception):
    pass


class SourceLoader:
    """ General class that provides mechanics to load source content into pandas/spark dataframes. """

    _mode_spark = "spark"
    _mode_pandas = "pandas"

    _valid_formats = {FORMAT_CSV, FORMAT_SAS}
    _valid_types = {TYPE_FLOAT: float, TYPE_INT: int, TYPE_STR: str}

    def __init__(
            self,
            source_path: Union[str, Path],
            mode: str,
            spark_session: Optional[ps.sql.SparkSession] = None
    ):
        """
        Initialize class instance.

        Args:
            source_path: path of input source data
            mode: 'pandas' or 'spark' mode
            spark_session: initialized SparkSession object
        """

        self._source_path = source_path
        self._mode = mode
        self._format = source_path.suffix if isinstance(source_path, Path) else "." + source_path.split(".")[-1]
        self._spark_session = spark_session

        if self._format not in self._valid_formats:
            raise SourceLoaderError(f"File suffix {self._format} invalid (allowed: {self._valid_formats})")

        if self._mode == self._mode_spark and not self._spark_session:
            raise SourceLoaderError(f"Expected a SparkSession object to be used with mode '{self._mode_spark}'")

        self._df: Union[pd.DataFrame, ps.sql.DataFrame, None] = None

    def load(self, csv_sep: str = ",") -> 'SourceLoader':
        """
        API method that loads data into dataframe (depending on chosen spark/pandas) mode and returns the current
        instance for chaining. Call this method first on a given SourceLoader instance to proceed.

        Args:
            csv_sep: separator of input CSV-files

        Returns:
            SourceLoader: current instance
        """

        if self._mode == self._mode_spark:
            if self._format == FORMAT_SAS:
                self._df = self._spark_session.read.format("com.github.saurfang.sas.spark").load(str(self._source_path))
            else:
                self._df = self._spark_session.read.csv(str(self._source_path), header=True, inferSchema=True,
                                                        sep=csv_sep)
        else:
            if self._format == FORMAT_SAS:
                self._df = pd.read_sas(self._source_path, FORMAT_SAS, encoding="ISO-8859-1")
            else:
                self._df = pd.read_csv(self._source_path, sep=csv_sep)

        return self

    def _df_col_to_type(
            self,
            col: str,
            type_: str,
            df: Union[pd.DataFrame, ps.sql.DataFrame]
    ) -> Union[pd.DataFrame, ps.sql.DataFrame]:
        """
        Private method used to transform dataframe columns to specific types.

        Args:
            col: column name
            type_: type as string (currently 'int', 'float', 'string')
            df: input pandas/spark dataframe object

        Returns:
            Union[pd.DataFrame, ps.sql.DataFrame]: tranformed dataframe
        """

        if type_ not in self._valid_types:
            raise SourceLoaderError(f"Invalid type {type_} (allowed: {', '.join(self._valid_types)}")

        if self._mode == self._mode_spark:
            return df.withColumn(col, F.col(col).cast(type_))
        else:
            return df.astype({col: type_})

    def _df_remove_duplicates(
            self,
            cols: Sequence[str],
            df: Union[pd.DataFrame, ps.sql.DataFrame]
    ) -> Union[pd.DataFrame, ps.sql.DataFrame]:
        """
        Private method to remove duplicates within dataframe.

        Args:
            cols: columns to check for duplicates
            df: dataframe

        Returns:
            Union[pd.DataFrame, ps.sql.DataFrame]: transformed dataframe
        """

        if self._mode == self._mode_spark:
            return df.dropDuplicates(subset=cols)
        else:
            return df[df.duplicated(subset=cols, keep="first") == False]

    def clean(
            self,
            dropna_cols: Optional[Sequence[str]] = None,
            to_type_cols: Optional[Dict[str, str]] = None,
            drop_duplicate_cols: Optional[Sequence[str]] = None,
            drop_cols: Optional[Sequence[str]] = None
    ) -> 'SourceLoader':
        """
        API method to clean the previously loaded dataframe via selected options.

        Args:
            dropna_cols: drop invalid values based on provided column names
            to_type_cols: cast provided columns into provided type (currently 'int', 'float', 'str')
            drop_duplicate_cols: drop duplicated rows for provided column names
            drop_cols: drop provided columns

        Returns:
            SourceLoader: current instance
        """

        if to_type_cols:
            for col, type_ in to_type_cols.items():
                self._df = self._df_col_to_type(col, type_, self._df)

        if drop_duplicate_cols:
            self._df = self._df_remove_duplicates(drop_duplicate_cols, self._df)

        if dropna_cols:
            self._df = self._df.dropna(subset=dropna_cols)

        if drop_cols:
            self._df = self._df.drop(columns=drop_cols) if self._mode == self._mode_pandas else self._df.drop(
                *drop_cols)

        return self

    def verify_data(self, min_rows: int = 10) -> 'SourceLoader':
        """ Performs some simple verification (>= 10 entries within loaded data).

        Args:
             min_rows: minimum number of rows within dataframe
        """

        if self._mode == self._mode_spark:
            count_tot = self._df.count()
            count_dup = self._df.distinct().count()

        else:
            count_tot = len(self._df)
            count_dup = len(self._df.unique())

        if count_tot < min_rows:
            log.warning(f"The data contains a suspiciously low number of records ({count_tot})")

        if count_tot != count_dup:
            log.warning(f"The data still contains duplicate rows ({count_tot} - {count_dup})")

        return self

    @property
    def df(self) -> Union[pd.DataFrame, ps.sql.DataFrame]:
        """ Getter for data dataframe of this instance. """

        if self._df is None:
            log.warning("This object has no initialized DataFrame - run load method first.")

        return self._df

    @df.setter
    def df(self, df: Union[pd.DataFrame, ps.sql.DataFrame]):
        """ Setter for data dataframe of this instance. """

        self._df = df
