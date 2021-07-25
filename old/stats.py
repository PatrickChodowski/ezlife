
import pandas as pd
from data_source import DataSource
import bokeh




class Stats:
    """
    Main file
    """
    def __init__(self,
                 x: str,
                 y: str,
                 csv_local_path: str = None,
                 bigquery_path: str = None,
                 data_frame: pd.DataFrame = None):

        self.data_source = DataSource(csv_local_path=csv_local_path,
                                      bigquery_path=bigquery_path,
                                      data_frame=data_frame)

        def plot_bar(self):
            p = figure(x_range=fruits, plot_height=250, title="Fruit counts",
                       toolbar_location=None, tools="")

            p.vbar(x=fruits, top=counts, width=0.9)

            p.xgrid.grid_line_color = None
            p.y_range.start = 0

            show(p)
