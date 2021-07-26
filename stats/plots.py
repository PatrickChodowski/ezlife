import matplotlib
import pandas as pd

matplotlib.use('tkagg')


class _Plots:
    """
    Class name with _ as its not supposed to be called directly

    Takes parameters about data, plot_type and additional plot settings.

    Returns pandas-matplotlib plot object
    """

    def __init__(self, df: pd.DataFrame, x: str, y: str):
        self.df = df
        self.x = x
        self.y = y

    def bar(self) -> None:
        self.df.plot.bar(x=self.x, y=self.y, rot=0)

