from .utils import GBQ, get_logger, PLOT_TYPES
from .query_builder import _QueryBuilder
from .plots import _Plots
import re
import os
from typing import Dict, List, Tuple
import pandas as pd


class GBQData:
    def __init__(self,
                 gbq_path: str,
                 sa_path: str):

        self.logger = get_logger('stats')

        # needed to check credentials first, gbq_path will check if table exist and needs connection ready
        self.sa_path = sa_path
        self.gbq_path = gbq_path

        # Query attributes
        self.query_builder = None
        self.query = None

        # used to move data from .set() to .get() method
        self.dimensions = None
        self.metrics = None
        self.aggregations = None

        # to access plots 'machine' ;d
        self.plots = None
        self.plots_created = list()

    @property
    def sa_path(self):
        return self._sa_path

    @sa_path.setter
    def sa_path(self, sa_path):
        """
        Validating if sa_path leads to correct service account
        """
        if sa_path is None:
            raise WrongServiceAccountPathException("Path to service account is empty")

        if not os.path.isfile(sa_path):
            raise WrongServiceAccountPathException(f"Did not find {sa_path}")

        if not sa_path.endswith('.json'):
            raise WrongServiceAccountPathException(f"File path {sa_path} doesnt end up with .json")

        self._sa_path = sa_path

    @property
    def gbq_path(self):
        return self._gbq_path

    @gbq_path.setter
    def gbq_path(self, gbq_path):
        """
        Validating if path is project.dataset.table format and if table exists
        """
        if gbq_path is None:
            raise GBQWrongPathPatternException("GBQ path is empty")

        if not re.search(r"([A-Z,a-z,0-9,\-,\_]+)\.([A-Z,a-z,0-9,\-,\_]+)\.([A-Z,a-z,0-9,\-,\_]+)", gbq_path):
            raise GBQWrongPathPatternException(f"GBQ path {gbq_path} doesnt seem to have project.dataset.table pattern")

        gbq_path_list = gbq_path.split('.')
        self.project_id = gbq_path_list[0]
        self.dataset_id = gbq_path_list[1]
        self.table_id = gbq_path_list[2]

        # setting up connection
        self._setup_gbq()
        if not self.gbq.check_if_table_exists(dataset_id=self.dataset_id, table_id=self.table_id):
            raise GBQTableNotExistException(f'GBQ table {gbq_path} doesnt exist')

        # returning dictionary of table column names and types
        self.cols = self._get_table_cols_dict()

        self._gbq_path = gbq_path

    def _setup_gbq(self) -> None:
        """
        Sets up GBQ client/utils if provided data source is GBQ table
        """
        self.gbq = GBQ(project_id=self.project_id,
                       sa_credentials=self.sa_path,
                       logger=self.logger)

    def _get_table_cols_dict(self) -> Dict[str, str]:
        """
        Gets given table metadata (columns and datatypes)
        :return: Dictionary of [column, datatype]
        """
        # self.gbq.get_table_schema returns pd.DataFrame
        cols_df = self.gbq.get_table_schema(dataset_id=self.dataset_id, table_id=self.table_id)
        return cols_df.set_index('column_name').to_dict()['data_type']

    def set(self,
            dimensions: List[str] = None,
            metrics: List[str] = None,
            aggregations: List[str] = None,
            sort: Tuple[str, str] = None,
            filters: List[Tuple] = None,
            limit: int = None
            ) -> None:
        """
        Builds _QueryBuilder, by sending all parameters needed for a query:

        :param dimensions: List of column dimensions (will be serving as groups) ['a','b']
        :param metrics: List of metrics to be aggregated (has to have at least one) ['c']
        :param aggregation: Aggregation type - one of [sum, avg, min, max, count]
        :param sort: Tuple of column name and ordering direction ('a', asc)
        :param filters: List of tuples of (column, operation, value(s))
        :param limit: Limit of how many rows to return from query
        :return: Query string to send
        """

        self.query_builder = _QueryBuilder(project_id=self.project_id,
                                           dataset_id=self.dataset_id,
                                           table_id=self.table_id,
                                           logger=self.logger,
                                           cols=self.cols,
                                           dimensions=dimensions,
                                           metrics=metrics,
                                           aggregations=aggregations,
                                           sort=sort,
                                           filters=filters,
                                           limit=limit)

        self.query = self.query_builder.glue_query()
        self.dimensions = dimensions
        self.metrics = metrics
        self.aggregations = aggregations

    def get(self, plot_type: str = None) -> pd.DataFrame:
        """
        Placeholder method for now.
        Sends self.query to gbq and retrieves pandas DF

        :param plot_type: Type of plot to show, one of: ['bar']
        :return: DataFrame with raw data as it can be useful too
        """
        df = self.gbq.get_data(query=self.query)
        # print(df.head())

        if plot_type in PLOT_TYPES:
            self.plots = _Plots(df=df,
                                dimensions=self.dimensions,
                                metrics=self.metrics,
                                aggregations=self.aggregations,
                                logger=self.logger)
            p = getattr(self.plots, plot_type)()
            self.plots_created.append(p)

        return df


class GBQWrongPathPatternException(Exception):
    pass


class GBQTableNotExistException(Exception):
    pass


class WrongServiceAccountPathException(Exception):
    pass
