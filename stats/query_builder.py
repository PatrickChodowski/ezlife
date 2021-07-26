import logging
from typing import List, Tuple, Dict
import numpy as np


AGGR_TYPES = ['avg', 'sum', 'min', 'max', 'count']
OPERAND_MAP = {'eq': '=',
               'ne': '!=',
               'gt': '>',
               'lt': '<',
               'ge': '>=',
               'le': '<=',
               'in': 'IN',
               'nin': 'NOT IN'}


class _QueryBuilder:
    def __init__(self,
                 project_id: str,
                 dataset_id: str,
                 table_id: str,
                 cols: Dict[str, str],
                 logger: logging.Logger,
                 dimensions: List[str] = None,
                 metrics: List[str] = None,
                 aggregation: str = None,
                 sort: Tuple[str, str] = None,
                 filters: List[Tuple] = None,
                 limit: int = None
                 ):
        """
        Class name with _ as its not supposed to be called directly

        Will build proper query based on parameters
        """

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.cols = cols

        self.dimensions = dimensions
        self.metrics = metrics
        self.aggregation = aggregation
        self.sort = sort
        self.logger = logger
        self.filters = filters
        self.limit = limit

    @property
    def aggregation(self):
        return self._aggregation

    @property
    def dimensions(self):
        return self._dimensions

    @property
    def metrics(self):
        return self._metrics

    @property
    def limit(self):
        return self._limit

    @aggregation.setter
    def aggregation(self, aggregation):
        if aggregation is not None:
            if aggregation not in AGGR_TYPES:
                raise WrongAggregationException(f"Aggregation {aggregation} has invalid value. Use one of {AGGR_TYPES}")
        self._aggregation = aggregation

    @dimensions.setter
    def dimensions(self, dimensions):

        if not isinstance(dimensions, list):
            raise NotAListException("Dimensions have to be of type List")

        if dimensions.__len__() < 1:
            raise EmptyListException("Dimensions list have to have at least one dimension inside")

        for x in dimensions:
            if x is None:
                raise WrongFieldName(f"Empty dimension value")

            if x is not None:
                if x not in self.cols:
                    raise WrongFieldName(f"X Field {x} not in data source columns")
        self._dimensions = dimensions

    @metrics.setter
    def metrics(self, metrics):
        for y in metrics:
            if y is None:
                raise WrongFieldName(f"Empty y value")

            if y is not None:
                if y not in self.cols:
                    raise WrongFieldName(f"Y Field {y} not in data source columns")
        self._metrics = metrics

    @limit.setter
    def limit(self, limit):
        if limit is not None:
            if not isinstance(limit, (int, np.int)):
                raise WrongLimitValue("Limit has to be of INT type")

            if limit <= 0:
                raise WrongLimitValue("Limit value has to be at least 1 or empty")

        self._limit = limit

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

                if operand not in OPERAND_MAP:
                    raise WrongFilterException(
                        f"Filter index [{index}] operand {operand} is not one of {list(OPERAND_MAP.keys())}")

                if (operand in ['in', 'nin']) & (not isinstance(values, list)):
                    raise WrongFilterException(
                        f"Filter index [{index}] values for operands ['in','nin'] has to be a list")

                if (operand not in ['in', 'nin']) & (not isinstance(values, (int, float, str, np.int, np.float))):
                    raise WrongFilterException(
                        f"""Filter index [{index}] values for operands ['eq','ne','gt','ge','le', 'lt'] 
                        has to be one of [int, float, str, np.int, np.float""")

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
        dim_str = ','.join(self.dimensions)
        comma_str = ", " if dim_str != "" else ""

        if self.aggregation is not None:
            self.logger.info(f"Grouping {self.metrics} by {self.dimensions}. Aggregation: {self.aggregation}")

            group_by_str = f" GROUP BY {dim_str} "
            metrics_str = ', '.join([f"{self.aggregation}({m}) AS {m}" for m in self.metrics])

            select_values_str = f" {dim_str}{comma_str}{metrics_str} "
            return group_by_str, select_values_str
        else:
            not_aggr_metrics_str = ','.join(self.metrics)
            group_by_str = ''
            select_values_str = f" {dim_str}{comma_str}{not_aggr_metrics_str} "
            return group_by_str, select_values_str

    def _sort_data(self) -> str:
        if self.sort is not None:

            if not isinstance(self.sort, tuple):
                raise WrongSortException(f"Sort has to be Tuple(column_name, asc/desc)")

            if self.sort.__len__() != 2:
                raise WrongSortException(f"Sort has {self.sort.__len__()} elements. Should have 2: column and asc/desc")

            if self.sort[1] not in ['asc', 'desc']:
                raise WrongSortException(f"Invalid sort value ({self.sort[1]}). It has to be one of ['asc','desc']")

            if self.sort[0] not in self.cols:
                raise WrongSortFieldNameException(f"Invalid sort column ({self.sort[0]}). Not found in columns")

            return f" ORDER BY {self.sort[0]} {self.sort[1].upper()} "
        else:
            return ''

    def _limit_data(self) -> str:
        if self.limit is not None:
            return f" LIMIT {self.limit}"
        else:
            return ''

    def glue_query(self) -> str:
        list_of_str_filters = self._filter_data()
        group_by_str, select_val_str = self._aggr_data()
        sort_by_str = self._sort_data()
        limit_str = self._limit_data()

        filter_str = "".join(list_of_str_filters)

        query = f"""
        SELECT {select_val_str} 
        FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        WHERE 1=1
            {filter_str} 
            {group_by_str} 
            {sort_by_str}
            {limit_str}"""
        self.logger.info("Final query: ")
        self.logger.info(query)

        return query


class WrongLimitValue(Exception):
    pass


class WrongAggregationException(Exception):
    pass


class WrongOperandException(Exception):
    pass


class WrongSortException(Exception):
    pass


class WrongSortFieldNameException(Exception):
    pass


class WrongFilterException(Exception):
    pass


class WrongFieldName(Exception):
    pass


class NotAListException(Exception):
    pass


class EmptyListException(Exception):
    pass
