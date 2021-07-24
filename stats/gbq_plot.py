import numpy as np
import pandas as pd
from typing import List, Dict, Tuple
from .utils import GBQ
import logging
import matplotlib
matplotlib.use('tkagg')

AGGR_TYPES = ['mean', 'sum', 'min', 'max', 'count', 'first', 'last', 'median', 'std', 'var', 'mad']
OPERAND_TYPES = ['eq', 'ne', 'gt', 'lt', 'ge', 'le', 'in', 'nin']
OPERAND_MAP = {'eq': '=',
               'ne': '!=',
               'gt': '>',
               'lt': '<',
               'ge': '>=',
               'le': '<=',
               'in': 'IN',
               'nin': 'NOT IN'}


class GBQPlot:
    """
    Will be handled differently than pandas as its more efficient
    to just do all necessary operations on GBQ level and download data just for vis
    """
    def __init__(self,
                 gbq_path: str,
                 x: str,
                 y: str,
                 logger: logging.Logger,
                 aggregation: str = None,
                 sort: str = None,
                 filters: list = None):

        self.gbq_path = gbq_path
        gbq_path_list = self.gbq_path.split('.')
        self.project_id = gbq_path_list[0]
        self.dataset_id = gbq_path_list[1]
        self.table_id = gbq_path_list[2]
        self.gbq = GBQ(project_id=self.project_id, sa_credentials='credentials/nbar.json', logger=logger)
        self.cols = self._get_available_columns()

        self.x = x
        self.y = y

        self.aggregation = aggregation
        self.sort = sort
        self.filters = filters
        self.logger = logger

        self.df = self._prep_and_send_query()


    @property
    def aggregation(self):
        return self._aggregation

    @property
    def sort(self):
        return self._sort

    @property
    def x(self):
        return self._x

    @property
    def y(self):
        return self._y

    @aggregation.setter
    def aggregation(self, aggregation):
        if aggregation is not None:
            if aggregation not in AGGR_TYPES:
                raise WrongAggregationException(f"Aggregation {aggregation} has invalid value. Use one of {AGGR_TYPES}")
        self._aggregation = aggregation

    @sort.setter
    def sort(self, sort):
        if sort is not None:
            if sort not in ['asc', 'desc']:
                raise WrongSortException(f"Sort {sort} has invalid value. Use one of ['asc','desc']")
        self._sort = sort

    @x.setter
    def x(self, x):
        if x is None:
            raise WrongFieldName(f"Empty x value")

        if x is not None:
            pass
            if x not in self.cols:
                raise WrongFieldName(f"X Field {x} not in data source columns")
        self._x = x

    @y.setter
    def y(self, y):
        if y is None:
            raise WrongFieldName(f"Empty y value")

        if y is not None:
            if y not in self.cols:
                raise WrongFieldName(f"Y Field {y} not in data source columns")
        self._y = y

    def _prep_and_send_query(self) -> pd.DataFrame:
        list_of_str_filters = self._filter_data()
        group_by_str, select_val_str = self._aggr_data()
        sort_by_str = self._sort_data()

        filter_str = "".join(list_of_str_filters)

        query = f"""SELECT {select_val_str} 
        FROM {self.gbq_path}
        WHERE 1=1
        {filter_str} 
        {group_by_str} {sort_by_str}"""
        self.logger.info("Final query: ")
        self.logger.info(query)

        df = self.gbq.get_data(query = query)
        return df

    def bar(self) -> None:
        self.df.plot.bar(x=self.x, y=self.y, rot=0)


    def _filter_data(self) -> List[str]:
        # column_name
        # operands = ['eq','gt','lt','ge','le', 'ne', 'in', 'nin']
        # value = list, tuple, string, int, float, has to be the same type as column
        # example filter is a tuple: (column_name, operator, values)
        if self.filters is not None:
            if not isinstance(self.filters, list):
                raise WrongFilterException("Filters have to be a list of tuples")
            if self.filters.__len__() == 0:
                raise WrongFilterException("Filters list can be either None or length greater than 0")

            filters_str_list = list()
            for index, f in enumerate(self.filters):
                if not isinstance(f, tuple):
                    raise WrongFilterException(f"Filter index[{index}] has to be a tuple(column_name, operand, values)")
                if f.__len__() != 3:
                    raise WrongFilterException(f"Filter index[{index}] has to be a tuple(column_name, operand, values)")

                col = f[0]
                operand = f[1]
                values = f[2]

                if col not in self.cols:
                    raise WrongFilterException(f"Filter index [{index}] column {col} not found in source df")

                if operand not in OPERAND_TYPES:
                    raise WrongFilterException(
                        f"Filter index [{index}] operand {operand} is not one of {OPERAND_TYPES}")

                if (operand in ['in', 'nin']) & (not isinstance(values, list)):
                    raise WrongFilterException(
                        f"Filter index [{index}] values for operands ['in','nin'] has to be a list")

                if (operand not in ['in', 'nin']) & (not isinstance(values, (int, float, str, np.int, np.float))):
                    raise WrongFilterException(
                        f"Filter index [{index}] values for operands ['eq','ne','gt','ge','le', 'lt'] has to be one of [int, float, str, np.int, np.float")

                self.logger.info(f"Applying filter index[{index}]: {col} {operand} {values}")

                qstr = ""  # to add quote or not
                if operand in ('eq', 'ne', 'le', 'lt', 'ge', 'gt'):
                    if isinstance(values, str):
                        qstr = "'"
                    filter_str = f'AND {col} {OPERAND_MAP[operand]} {qstr}{values}{qstr} '


                elif operand in ['in', 'nin']:
                    if isinstance(values[0], str):
                        qstr = "'"
                    values_str = f"({qstr}" + f"{qstr},{qstr}".join(values) + f"{qstr})"
                    filter_str = f'AND {col} {OPERAND_MAP[operand]} {values_str} '

                filters_str_list.append(filter_str)
            return filters_str_list
        else:
            return list()

    def _aggr_data(self) -> Tuple[str, str]:
        if self.aggregation is not None:
            self.logger.info(f"Grouping {self.y} by {self.x}. Aggregation: {self.aggregation}")
            group_by_str = f" GROUP BY {self.x} "
            select_values_str = f" {self.x}, {self.aggregation}({self.y}) AS {self.y} "
            return group_by_str, select_values_str
        else:
            group_by_str = ''
            select_values_str = f" {self.x}, {self.y} "
            return group_by_str, select_values_str

    def _sort_data(self) -> str:
        if self.sort is not None:
            self.logger.info(f"Sorting {self.sort} by {self.y}")
            sort_by_str = f" ORDER BY {self.y} {self.sort.upper()} "
            return sort_by_str
        else:
            return ''

    def _get_available_columns(self) -> list:
        col_list = list(self.gbq.get_table_schema(dataset_id=self.dataset_id,
                                                  table_id=self.table_id)['column_name'].values)
        return col_list



class WrongAggregationException(Exception):
    pass


class WrongOperandException(Exception):
    pass


class WrongSortException(Exception):
    pass


class WrongFilterException(Exception):
    pass


class WrongFieldName(Exception):
    pass

