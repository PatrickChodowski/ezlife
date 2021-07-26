import logging
from typing import List, Tuple, Dict
import numpy as np


OPERAND_MAP = {'eq': '=',
               'ne': '!=',
               'gt': '>',
               'lt': '<',
               'ge': '>=',
               'le': '<=',
               'in': 'IN',
               'nin': 'NOT IN'}

AGGR_MAP = {"avg": "AVG(",
            "sum": "SUM(",
            "min": "MIN(",
            "max": "MAX(",
            "count": "COUNT(",
            "count_distinct": "COUNT(DISTINCT "}


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

        Takes parameters about dimensions, metrics, aggregations, sorting,
        filters and limits and builds an SQL Query out of it.

        Performs multiple validation on each parameter before query writing
        to reduce number of times we send a faulty query.

        If any of the validation tests fails, an exception is raised
        instead of trying to overcome it and send simplified query.
        It's important for user to be exactly 100% sure what data are they looking at

        Main method is glue_query, which will return a full query string
        """
        # table information
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.cols = cols

        # query parameters
        self.dimensions = dimensions
        self.metrics = metrics
        self.aggregation = aggregation
        self.sort = sort
        self.logger = logger
        self.filters = filters
        self.limit = limit

    # properties:
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

    @property
    def filters(self):
        return self._filters

    @property
    def sort(self):
        return self._sort

    # setters:
    @aggregation.setter
    def aggregation(self, aggregation):
        """
        Aggregation checks:
        - cant be outside of AGGR_MAP
        - cant be empty (at least for now)
        """
        if aggregation is None:
            raise WrongAggregationException(f"Aggregation cant be empty")

        if aggregation not in AGGR_MAP:
            raise WrongAggregationException(f"""Aggregation {aggregation} has invalid value. 
            Use one of {list(AGGR_MAP.keys())}""")
        self._aggregation = aggregation

    @dimensions.setter
    def dimensions(self, dimensions):
        """
        List of dimensions to group by:
        - has to be a list or None (aggregating without grouping)
        - If its a list, then it has has to have at least 1 element

        Checks on dimensions:
        - have to be in column list
        - cant be empty
        """
        if dimensions is not None:
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
        """
        List of metrics to aggregate:
        - cant be None
        - has to be a list
        - has to have at least 1 element

        Checks on metrics:
        - have to be in column list
        - cant be empty
        """
        if metrics is None:
            raise NotAListException("At least one metric has to be provided")

        if not isinstance(metrics, list):
            raise NotAListException("Metrics have to be a list")

        if metrics.__len__() < 1:
            raise EmptyListException("Metrics list has to have at least 1 element")

        for y in metrics:
            if y is None:
                raise WrongFieldName(f"Empty y value")

            if y is not None:
                if y not in self.cols:
                    raise WrongFieldName(f"Y Field {y} not in data source columns")
        self._metrics = metrics

    @filters.setter
    def filters(self, filters):
        """
        Filter checks

        - can be none
        - if not None, it has to be a list of tuples
        - each tuple has to have 3 elements (column_name, operator, value)

        - Each column has to be in column list
        - Each operator has to be one of ['eq','gt','lt','ge','le', 'ne', 'in', 'nin']
        - Value can be List for 'in' and 'nin' operators
        - For others value can be a string, int or float
        """
        if filters is not None:
            if not isinstance(filters, list):
                raise WrongFilterException("Filters have to be a list of tuples")
            if filters.__len__() == 0:
                raise WrongFilterException("Filters list can be either None or length greater than 0")

            for index, f in enumerate(filters):
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

        self._filters = filters

    @limit.setter
    def limit(self, limit):
        """
        Limit checks:
        - if its not empty, has to be int
        - if int, has to be greater than 1
        """
        if limit is not None:
            if not isinstance(limit, (int, np.int)):
                raise WrongLimitValue("Limit has to be of INT type")

            if limit <= 0:
                raise WrongLimitValue("Limit value has to be at least 1 or empty")

        self._limit = limit

    @sort.setter
    def sort(self, sort):
        """
        Sort checks:
        - Can be empty
        - If not empty, it has to be a Tuple
        - Tuple has to have 2 elements = column name and sort direction
        - Second element of tuple has to be either asc or desc
        - First element of tuple has to be a column name and has to be in self.cols
        - It also has to be in either self.dimensions or self.metrics
        """
        if sort is not None:

            if not isinstance(sort, tuple):
                raise WrongSortException(f"Sort has to be Tuple(column_name, asc/desc)")

            if sort.__len__() != 2:
                raise WrongSortException(f"Sort has {sort.__len__()} elements. Should have 2: column and asc/desc")

            if sort[1] not in ['asc', 'desc']:
                raise WrongSortException(f"Invalid sort value ({sort[1]}). It has to be one of ['asc','desc']")

            if sort[0] not in self.cols:
                raise WrongSortFieldNameException(f"Invalid sort column ({sort[0]}). Not found in columns")

            if (sort[0] not in self.metrics) & (sort[0] not in self.dimensions):
                raise WrongSortFieldNameException(f"""Invalid sort column ({sort[0]}). 
                Not found in dimensions or metrics""")
        self._sort = sort

    # prepare query methods:
    def _filter_data(self) -> str:
        """
        Creates filter strings (like pts > 1) from self.filters
        :return: Joined filter string or empty string if filters are not specified
        """
        if self.filters is not None:
            filters_str_list = list()
            for index, f in enumerate(self.filters):
                col = f[0]
                operand = f[1]
                values = f[2]
                self.logger.info(f"Applying filter index[{index}]: {col} {operand} {values}")

                qstr = ""  # to add quote or not
                if operand in ('eq', 'ne', 'le', 'lt', 'ge', 'gt'):
                    if isinstance(values, str):
                        qstr = "'"
                    filter_str = f'AND {col} {OPERAND_MAP[operand]} {qstr}{values}{qstr} '
                else:
                    if isinstance(values[0], str):
                        qstr = "'"
                    values_str = f"({qstr}" + f"{qstr},{qstr}".join(values) + f"{qstr})"
                    filter_str = f'AND {col} {OPERAND_MAP[operand]} {values_str} '

                filters_str_list.append(filter_str)
            return "".join(filters_str_list)
        else:
            return ""

    def _aggr_data(self) -> Tuple[str, str]:
        """
        Creates SELECT and GROUP BY strings based on self.dimensions, self.metrics and aggregation params

        I tried to make it so it handles any combination of:
            - single, multiple or none dimensions
            - single or multiple metrics (empty metrics not allowed)
            - different aggregations (for now having very simple ones)
        :return: Tuple of select string, group by string
        """
        if self.dimensions is not None:
            dim_str = ','.join(self.dimensions)
            comma_str = ", "
            group_by_pre_str = "GROUP BY "
        else:
            dim_str = ""
            comma_str = ""
            group_by_pre_str = ""

        if self.aggregation is not None:
            self.logger.info(f"Grouping {self.metrics} by {self.dimensions}. Aggregation: {self.aggregation}")
            aggr_str = AGGR_MAP[self.aggregation]
            group_by_str = f" {group_by_pre_str}{dim_str} "
            metrics_str = ', '.join([f"{aggr_str}{m}) AS {m}" for m in self.metrics])

            select_values_str = f" {dim_str}{comma_str}{metrics_str} "
            return select_values_str, group_by_str
        else:
            not_aggr_metrics_str = ','.join(self.metrics)
            group_by_str = ''
            select_values_str = f" {dim_str}{comma_str}{not_aggr_metrics_str} "
            return select_values_str, group_by_str

    def _sort_data(self) -> str:
        """
        Creates ORDER BY string from self.sort tuple
        :return: ORDER BY string or empty string if there is no order clause
        """
        if self.sort is not None:
            return f" ORDER BY {self.sort[0]} {self.sort[1].upper()} "
        else:
            return ''

    def _limit_data(self) -> str:
        """
        Creates LIMIT string from self.limit variable
        :return: Limit string or empty string if no limit is provided (NO LIMITS! wooo)
        """
        if self.limit is not None:
            return f" LIMIT {self.limit}"
        else:
            return ''

    def glue_query(self) -> str:
        """
        Joins final query. Uses specific method for each part of query:
        _filter_data (WHERE clause)
        _aggr_data   (GROUP BY and SELECT clauses)
        _sort_data   (ORDER BY clause)
        _limit_data  (LIMIT cluase)
        To prepare its part of query and then builds the final query string
        :return: Query string
        """
        filter_str = self._filter_data()
        select_val_str, group_by_str = self._aggr_data()
        sort_by_str = self._sort_data()
        limit_str = self._limit_data()

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
