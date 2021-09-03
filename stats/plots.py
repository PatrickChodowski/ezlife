import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import logging
from typing import List, Union
import itertools

matplotlib.use('tkagg')


class _Plots:
    """
    Class name with _ as its not supposed to be called directly

    Takes parameters about df, dimensions, metrics and aggregations
    Later will pass kwargs for plot specific settings

    Returns pandas-matplotlib plot object
    """

    def __init__(self,
                 df: pd.DataFrame,
                 dimensions: List[str],
                 metrics: List[str],
                 aggregations: List[str],
                 logger: logging.Logger):

        self.df = df
        self.dimensions = dimensions
        self.metrics = metrics
        self.aggregations = aggregations

        self.metric_cols = self._get_possible_metrics()
        self.logger = logger

    def _prep_groups(self) -> Union[str, None]:
        """
        Prepares data for category. If none, returns empty string
        return Name of the grouping column or None if there is no dimensions
        """
        # if dimensions length are over 1 then they get concatenated into 'group'
        if self.dimensions is not None:
            if self.dimensions.__len__() > 1:
                x = 'group'
                self.df[x] = self.df[self.dimensions].agg('-'.join, axis=1)
            else:
                # if not, just using provided dimension
                x = self.dimensions[0]
            return x
        else:
            return None

    def bar(self) -> None:
        """
        Produces the bar plot with one category (X) and one metric (Y), based on:
        - self.df: data source
        - self.dimensions: concat of dimensions
        - self.metrics: picks the first one
        """
        x = self._prep_groups()
        if x is None:
            raise PlotRequiresCategoryException("No dimension provided for barchart")

        y = self.metric_cols[0]
        self.logger.info(f"Bar Plot with x: {x} and y: {y}")

        # rotate labels if too many groups
        #_rot = 0 if set(self.df[x].values).__len__() <= 10 else 90
        self.df.plot.bar(x=x,
                         y=y,
                         rot=0)

    def barh(self) -> None:
        """
        Produces the horizontal bar plot with one category (X) and one metric (Y), based on:
        - self.df: data source
        - self.dimensions: concat of dimensions
        - self.metrics: picks the first one
        """
        x = self._prep_groups()
        if x is None:
            raise PlotRequiresCategoryException("No dimension provided for barchart")

        y = self.metric_cols[0]
        self.logger.info(f"Bar Plot with x: {x} and y: {y}")
        self.df.plot.barh(x=x,
                          y=y,
                          rot=0)

    def boxplot(self) -> None:
        """
        Since I am pre-calculating statistics, I got to draw boxplot manually :) <- hold the pain face

        Boxplot requires median, q1 and q3 to be added to aggregations
        """

        if (('median' not in self.aggregations) | ('q1' not in self.aggregations) | ('q3' not in self.aggregations) |
                ('min_run' not in self.aggregations) | ('max_run' not in self.aggregations)):
            raise BoxplotMissingAggregationsException("Boxplot requires Q1, Median, Q3, min, max in aggregations")

        x = self._prep_groups()
        if x is None:
            PlotRequiresCategoryException("No dimension provided for boxplot")

        # picking first metric
        y = self.metrics[0]

        self.df[f'iqr_{y}'] = self.df[f'q3_{y}'] - self.df[f'q1_{y}']

        if x is None:
            x = ''
            self.df[x] = 0
        groups = self.df[[x, f'q1_{y}', f'median_{y}', f'q3_{y}', f'iqr_{y}', f'min_run_{y}', f'max_run_{y}']].to_dict('records')
        stats = list()

        for group in groups:
            whislo = max([(group[f'q1_{y}'] - (1.5 * group[f'iqr_{y}'])), group[f'min_run_{y}']])
            whishi = min([(group[f'q3_{y}'] + (1.5*group[f'iqr_{y}'])), group[f'max_run_{y}']])

            group_dict = {
                "label": group[x],
                "med":  group[f'median_{y}'],
                "q1": group[f'q1_{y}'],
                "q3": group[f'q3_{y}'],
                "whislo": whislo,  # required
                "whishi": whishi,  # required
                "fliers": []  # required if showfliers=True
            }
            stats.append(group_dict)

        fs = 10  # fontsize
        fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6, 6))
        axes.bxp(stats)
        #axes.set_title('Boxplot for precalculated statistics', fontsize=fs)
        plt.show()

    def scatter(self) -> None:
        """
        Produces the scatter plot with 2 metrics
        - self.df: data source
        - self.dimensions: Used only as a label
        - self.metrics: picks the first two
        """
        if self.metric_cols.__len__() < 2:
            raise ScatterplotTooFewMetrics(f"""Scatter plot requires at least 2 metrics. 
            Found ({self.metric_cols.__len__()})""")

        x = self._prep_groups()
        y1 = self.metric_cols[0]
        y2 = self.metric_cols[1]
        self.logger.info(f"Scatter Plot with y1: {y1} and y2: {y2}")

        # labeled version
        if x is not None:
            ax = self.df.plot(kind='scatter', x=y1, y=y2)
            self.df[[y1, y2, x]].apply(lambda row: ax.text(*row), axis=1)
        else:
            # non labeled version
            self.df.plot.scatter(x=y1, y=y2)

    def _get_possible_metrics(self) -> list:
        """
        Generates list of possible metrics
        cartesian product of aggregations and metrics
        :return: List of possible metrics
        """
        possible_metrics = list()
        for t in set(itertools.product(self.aggregations, self.metrics)):
            possible_metrics.append(f"{t[0]}_{t[1]}")
        return possible_metrics


class BoxplotMissingAggregationsException(Exception):
    pass


class ScatterplotTooFewMetrics(Exception):
    pass


class PlotRequiresCategoryException(Exception):
    pass
