from stats.data_source import DataSource
import pandas as pd
from stats.pandas_plot import PandasPlot
from stats.gbq_plot import GBQPlot
from stats.utils import get_logger


logger = get_logger('plots')


csv_path = 'data/traditional.csv'
gbq_path = 'nbar-264220.nbar_data.traditional'
#df = pd.read_csv(csv_path)


gp = GBQPlot(gbq_path=gbq_path,
             x='team_abbreviation',
             y='pts',
             aggregation='sum',
             sort='desc',
             logger=logger)
gp.bar()




pp = PandasPlot(df=df,
                x='team_abbreviation',
                y='pts',
                aggregation='mean',
                sort='desc',
                filters=[('team_abbreviation', 'in', ['WAS', 'DEN']),
                         ('fga', 'gt', 10)
                         ],
                logger=logger
                )
pp.bar()


# #ds = DataSource(csv_local_path=csv_path)
# #ds2 = DataSource(bigquery_path='nbar-264220.nbar_data.traditional')
# ds3 = DataSource(data_frame=df)
#
#
# ds3 = DataSource()



df.groupby('team_abbreviation')['pts'].agg('mean').reset_index()

values = ['WAS', 'DEN']
qstr = "'"
values_str = f"({qstr}" + f"{qstr},{qstr}".join(values) + f"{qstr})"
print(values_str)
