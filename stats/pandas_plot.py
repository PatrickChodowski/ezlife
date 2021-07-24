import numpy as np
import pandas as pd
import logging
import matplotlib
matplotlib.use('tkagg')

AGGR_TYPES = ['mean', 'sum', 'min', 'max', 'count', 'first', 'last', 'median', 'std', 'var', 'mad']
OPERAND_TYPES = ['eq', 'ne', 'gt', 'lt', 'ge', 'le', 'in', 'nin']


class PandasPlot:
    def __init__(self,
                 df: pd.DataFrame,
                 x: str,
                 y: str,
                 logger: logging.Logger,
                 aggregation: str = None,
                 sort: str = None,
                 filters: list = None):

        self.df = df
        self.df2 = df
        self.x = x
        self.y = y

        self.aggregation = aggregation
        self.sort = sort
        self.filters = filters
        self.logger = logger

        self._prep_data()

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
            if x not in self.df.columns.to_list():
                raise WrongFieldName(f"X Field {x} not in data source columns")
        self._x = x

    @y.setter
    def y(self, y):
        if y is None:
            raise WrongFieldName(f"Empty y value")

        if y is not None:
            if y not in self.df.columns.to_list():
                raise WrongFieldName(f"Y Field {y} not in data source columns")
        self._y = y

    def bar(self) -> None:
        self.df2.plot.bar(x=self.x, y=self.y, rot=0)

    def _prep_data(self) -> None:
        self._filter_data()
        self._aggr_data()
        self._sort_data()

    def _filter_data(self):
        # column_name
        # operands = ['eq','gt','lt','ge','le', 'ne', 'in', 'nin']
        # value = list, tuple, string, int, float, has to be the same type as column
        # example filter is a tuple: (column_name, operator, values)

        if self.filters is not None:
            if not isinstance(self.filters, list):
                raise WrongFilterException("Filters have to be a list of tuples")
            if self.filters.__len__() == 0:
                raise WrongFilterException("Filters list can be either None or length greater than 0")

            for index, f in enumerate(self.filters):
                if not isinstance(f, tuple):
                    raise WrongFilterException(f"Filter index[{index}] has to be a tuple(column_name, operand, values)")
                if f.__len__() != 3:
                    raise WrongFilterException(f"Filter index[{index}] has to be a tuple(column_name, operand, values)")

                col = f[0]
                operand = f[1]
                values = f[2]

                if col not in self.df.columns.to_list():
                    raise WrongFilterException(f"Filter index [{index}] column {col} not found in source df")

                if operand not in OPERAND_TYPES:
                    raise WrongFilterException(f"Filter index [{index}] operand {operand} is not one of {OPERAND_TYPES}")

                if (operand in ['in', 'nin']) & (not isinstance(values, (list, tuple, set))):
                    raise WrongFilterException(
                        f"Filter index [{index}] values for operands ['in','nin'] has to be one of [list, tuple, set]")

                if (operand not in ['in', 'nin']) & (not isinstance(values, (int, float, str, np.int, np.float))):
                    raise WrongFilterException(
                        f"Filter index [{index}] values for operands ['eq','ne','gt','ge','le', 'lt'] has to be one of [int, float, str, np.int, np.float")

                self.logger.info(f"Applying filter index[{index}]: {col} {operand} {values}")
                self.logger.info(f"NROWS before filter index[{index}]: {self.df2.shape[0]}")
                if operand in ('eq', 'ne', 'le', 'lt', 'ge', 'gt'):
                    self.logger.info(self.df2.shape)
                    self.df2 = self.df2.loc[(getattr(self.df2[col], operand)(values))]
                    self.logger.info(self.df2.shape)
                elif operand == 'in':
                    self.df2 = self.df2.loc[self.df2[col].isin(values)]
                elif operand == 'nin':
                    self.df2 = self.df2.loc[~self.df2[col].isin(values)]
                self.logger.info(f"NROWS after filter index[{index}]: {self.df2.shape[0]}")


    def _aggr_data(self):
        if self.aggregation is not None:
            self.logger.info(f"Grouping {self.y} by {self.x}. Aggregation: {self.aggregation}")
            self.df2 = self.df2.groupby(self.x)[self.y].agg(self.aggregation).reset_index()


    def _sort_data(self):
        if self.sort is not None:
            self.logger.info(f"Sorting {self.sort} by {self.y}")
            if self.sort == 'asc':
                self.df2.sort_values(by=self.y, ascending=True, inplace=True)
            elif self.sort == 'desc':
                self.df2.sort_values(by=self.y, ascending=False, inplace=True)



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

