import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import logging
from typing import List, Tuple

matplotlib.use('tkagg')


class _Plots:
    """
    Class name with _ as its not supposed to be called directly

    Takes parameters about data, plot_type and additional plot settings.

    Returns pandas-matplotlib plot object
    """

    def __init__(self,
                 df: pd.DataFrame,
                 dimensions: List[str],
                 metrics: List[str],
                 logger: logging.Logger):

        self.df = df
        self.dimensions = dimensions
        self.metrics = metrics
        self.logger = logger

    def _prep_x_y(self) -> Tuple[str, str]:
        """
        Prepares data for category X metric plot (bar, barh, boxplot)
        return Tuple with x and y (x,y)
        """
        # if dimensions length are over 1 then they get concatenated into 'group'
        if self.dimensions.__len__() > 1:
            x = 'group'
            self.df[x] = self.df[self.dimensions].agg('-'.join, axis=1)

        else:
            # if not, just using provided dimension
            x = self.dimensions[0]

        # picking first metric only (I allow only one metric per bar plot)
        y = self.metrics[0]
        return x, y

    def bar(self) -> None:
        """
        Produces the bar plot with one category (X) and one metric (Y), based on:
        - self.df: data source
        - self.dimensions: concat of dimensions
        - self.metrics: picks the first one
        """
        x, y = self._prep_x_y()
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
        x, y = self._prep_x_y()
        self.logger.info(f"Bar Plot with x: {x} and y: {y}")
        self.df.plot.barh(x=x,
                          y=y,
                          rot=0)

    def boxplot(self) -> None:
        """
        Since I am pre-calculating statistics, I got to draw boxplot manually :) <- hold the pain face
        """

        x, y = self._prep_x_y()
        groups = set(self.df[x].values)
        stats = list()
        for group in groups:
            group_dict = {
                "label": group,
                "med": 5.5,
                "q1": 3.5,
                "q3": 7.5,
                "whislo": 2.0,  # required
                "whishi": 8.0,  # required
                "fliers": []  # required if showfliers=True
            }
            stats.append(group_dict)

        fs = 10  # fontsize
        fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6, 6))
        axes.bxp(stats)
        #axes.set_title('Boxplot for precalculated statistics', fontsize=fs)
        plt.show()



