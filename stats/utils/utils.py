
OPERAND_MAP = {'eq': '=',
               'ne': '!=',
               'gt': '>',
               'lt': '<',
               'ge': '>=',
               'le': '<=',
               'in': 'IN',
               'nin': 'NOT IN'}

AGGR_MAP = {"avg": "AVG(__metric__)",
            "sum": "SUM(__metric__)",
            "min": "MIN(__metric__)",
            "max": "MAX(__metric__)",
            "count": "COUNT(__metric__)",
            "count_distinct": "COUNT(DISTINCT __metric__)",
            "count_nulls": "SUM(CASE WHEN __metric__ IS NULL THEN 1 ELSE 0 END)",
            "any": "ANY_VALUE(__metric__)",
            "string_agg": "STRING_AGG(__metric__)",
            "array_agg": "ARRAY_AGG(__metric__)",
            "string_agg_distinct": "STRING_AGG(DISTINCT __metric__)",
            "array_agg_distinct": "ARRAY_AGG(DISTINCT __metric__)",
            "median": "PERCENTILE_CONT(__metric__, 0.5) __over__",
            "q1": "PERCENTILE_CONT(__metric__, 0.25) __over__",
            "q3": "PERCENTILE_CONT(__metric__, 0.75) __over__",
            "min_run": "MIN(__metric__) __over__",
            "max_run": "MAX(__metric__) __over__",
            "stdev": "STDDEV_POP(__metric_) __over__",
            "var": "VAR_POP(__metric_) __over__"
            }

WINDOW_AGGRS = ["q1", "median", "q3", "stdev", "var", "min_run", "max_run"]

PLOT_TYPES = ["bar", "barh", "boxplot", "scatter"]
