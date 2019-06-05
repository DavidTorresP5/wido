import logging
import os

import pandas as pd
import numpy as np

from src import utils


# pandas float numbers with 2 floating points
pd.set_option('display.float_format', lambda x: '%.2f' % x)

"""
Logging
-------
We create a logger with a formatter and a handler and save logs at info level
"""

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s|%(name)s|%(levelname)s|%(message)s")
# Save class constructor logs to file: logs/dataproc.log
file_handler = logging.FileHandler("logs/dataproc.log")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


class Data():

    """
    Data
    -------
    class

    params:
    - file: a .csv, .json, .xlsx file or pathfile.
    - sep: column separator, if needed.
    - encoding (utf-8): text encoding.

    """

    def __init__(self, file, sep="", encoding="utf-8"):
        self.file = file
        self.sep = sep
        self.encoding = encoding
        self.extension = os.path.splitext(file)[1]
        self._df = self._input()



    def __repr__(self):
        return f"{self.file} listo!!"

    def _read_csv(self):
        """
        Private function to read csv files
        """
        return pd.read_csv(self.file, sep = self.sep, encoding= self.encoding)

    def _read_json(self):
        """
        Private function to read json files
        """
        return pd.read_json(self.file)

    def _read_excel(self):
        """
        Private function to read excel files
        """
        return pd.read_excel(self.file)


    @utils.timer
    def _input(self):
        """
        Emulates a switch statement with a dict.
        Uses a read private function depending of file extension
        """
        extension_dict = {
            ".csv": self._read_csv,
            ".txt": self._read_csv,
            ".excel": self._read_excel,
            ".json": self._read_json
        }

        func = extension_dict.get(self.extension, 
                                lambda: "Invalid extension")
        return func()


    def _detect_outliers(self, col, coef = 1.5):
        """
        Private function that read a dataframe column and returns a T/F series with outliers 
        A greater coef will return less outliers
        """

        q1, q3 = np.percentile(col.dropna(),[25,75])
        iqr = q3 - q1
        lower_bound = q1 - (coef * iqr) 
        upper_bound = q3 + (coef * iqr)
        col = col.fillna(np.mean(col.dropna()))
        return [~(lower_bound <= n <= upper_bound) for n in col]


    def _replace_outliers(self, col, coef = 1.5):
        """
        Private function that read a dataframe column and returns a series 
        with outliers replaced by 25th or 75th percentile 
        A greater coef will modify less outliers
        """

        if col.dtypes in ('float64', 'int64'):

            q1, q3 = np.percentile(col.dropna(),[25,75])
            iqr = q3 - q1
            lower_bound = q1 - (coef * iqr) 
            upper_bound = q3 + (coef * iqr)
            col = np.where(col > upper_bound, upper_bound, col)
            col = np.where(col < lower_bound, lower_bound, col)
            return col
        
        else:
            return col


    def show_sample(self, n_rows=3):
        """
        Sample n_rows of the dataframe
        """
        logger.info(f"Viewing sample table for {self.file}")
        return self._df.sample(n_rows)

    def show_summary(self):
        """
        Return some basic stats of a dataframe
        """
        logger.info(f"Viewing data quality tables for {self.file}")
        return self._df.describe().round(2)

    def show_na_by_col(self):
        """
        Return each column name with the number of NAs
        """
        return self._df.isnull().sum()

    def show_num_cols_info(self):
        """
        Create a dataframe with 1st, 25th, 50th, 75th and 99th percentiles as columns as
        well as another column with outliers' percentage
        """
        df_nums = self._df.quantile([.01, 0.25, .50, 0.75, 0.99]).transpose()
        df_nums = df_nums.add_prefix("perc_")
        
        subset_nums = self._df.select_dtypes(include=['float64', 'int64'])
        df_nums['PercentageOut'] = subset_nums.apply(self._detect_outliers, axis = 0).mean().round(4) * 100
        return df_nums

    def show_dataqual_info(self):
        """
        Create a df with datatype, number of unique values and % of null values per column
        """
        df = pd.DataFrame({
            'DataType': self._df.dtypes,
            'UniqueValues': self._df.nunique(),
            'PercentageNA': self._df
                .isna()
                .mean()
                .round(4) * 100
                })
        return df

