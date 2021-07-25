import pandas as pd
import os
import re
from .utils import GBQ, get_logger


class _DataSource:
    def __init__(self,
                 csv_local_path: str = None,
                 bigquery_path: str = None,
                 data_frame: pd.DataFrame = None
                 ):
        """
        Handler for the data source. Currently can read data from local CSV file, BigQuery Table and Pandas Dataframe
        Dependent on what operation and what source user is doing, it will be handled differently

        :arg csv_local_path: Path to local csv file
        :arg bigquery_path: GBQ path in  'project.dataset.table' pattern
        :arg data_frame: Name of pandas dataframe

        """
        # checks if there is correct number (1) of provided data sources
        self._raise_if_multiple_args(csv_local_path,
                                     bigquery_path,
                                     data_frame)

        # data source params
        self.csv_local_path = csv_local_path
        self.bigquery_path = bigquery_path
        self.data_frame = data_frame

        self.data_source_type = None

        # additional utils based on data source
        self.gbq = None

    @property
    def csv_local_path(self):
        return self._csv_local_path

    @property
    def bigquery_path(self):
        return self._bigquery_path

    @property
    def data_frame(self):
        return self._data_frame

    @csv_local_path.setter
    def csv_local_path(self, csv_local_path):
        """
        Validating if path exists and if contains .csv at the end
        """
        if csv_local_path is not None:
            if not os.path.isfile(csv_local_path):
                raise CSVWrongPathException(f"Did not find {csv_local_path}")
            if not csv_local_path.endswith('.csv'):
                raise CSVWrongSuffixException(f"File path {csv_local_path} doesnt end up with .csv")
        self._csv_local_path = csv_local_path

    @bigquery_path.setter
    def bigquery_path(self, bigquery_path):
        """
        Validating if path is project.dataset.table format and if table exists
        """
        if bigquery_path is not None:
            if not re.search(r"([A-Z,a-z,0-9,\-,\_]+)\.([A-Z,a-z,0-9,\-,\_]+)\.([A-Z,a-z,0-9,\-,\_]+)", bigquery_path):
                raise GBQWrongPathPatternException(f"GBQ path {bigquery_path} doesnt seem to have project.dataset.table pattern")

            gbq_path_list = bigquery_path.split('.')
            project_id = gbq_path_list[0]
            dataset_id = gbq_path_list[1]
            table_id = gbq_path_list[2]

            self._setup_gbq(project_id=project_id)
            if not self.gbq.check_if_table_exists(dataset_id=dataset_id, table_id=table_id):
                raise GBQTableNotExist(f'GBQ table {bigquery_path} doesnt exist')

        self._bigquery_path = bigquery_path


    @data_frame.setter
    def data_frame(self, data_frame):
        """
        Validating if path exists and if contains .csv at the end
        """
        if data_frame is not None:
            if not isinstance(data_frame, pd.DataFrame):
                raise NotPandasDFException(f"Object for data_frame is of type {type(data_frame)}, not pd.DataFrame")
        self._data_frame = data_frame


    def _setup_gbq(self, project_id: str) -> None:
        """
        Sets up GBQ client if provided data source is GBQ table
        :param project_id: GBQ project id
        """
        self.gbq = GBQ(project_id=project_id, sa_credentials='credentials/nbar.json', logger=get_logger('gbq'))

    @staticmethod
    def _raise_if_multiple_args(csv_local_path, bigquery_path, data_frame):
        """
        Checks if there is exactly one data source provided
        """
        arg_list = [csv_local_path, bigquery_path, data_frame]
        non_empty_source_count = sum(1 for a in arg_list if a is not None)
        if non_empty_source_count != 1:
            raise WrongNumberOfDataSourcesException(f"""Wrong number of sources provided ({non_empty_source_count}) 
            please provide exactly one""")



class WrongNumberOfDataSourcesException(Exception):
    pass


class CSVWrongPathException(Exception):
    pass


class CSVWrongSuffixException(Exception):
    pass


class GBQWrongPathPatternException(Exception):
    pass


class GBQTableNotExist(Exception):
    pass


class NotPandasDFException(Exception):
    pass
