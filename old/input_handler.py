
import logging
from .data_source import _DataSource

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



class _InputHandler:
    def __init__(self,
                 x: str,
                 y: str,
                 aggregation: str = None,
                 sort: str = None,
                 logger: logging.Logger = None,
                 ):
        """
        _InputHandler will focus on checking aggregation, sort, x, y, properties
        By no means its supposed to be accessed directly by the user.
        Its inner class for Plots classes, thus _ in the name
        """

        self.x = x
        self.y = y
        self.aggregation = aggregation
        self.sort = sort
        self.logger = logger


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
