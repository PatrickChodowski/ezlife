from stats.data_source import DataSource
import pandas as pd
from stats.pandas_plot import PandasPlot
from stats.utils import get_logger


logger = get_logger('plots')


csv_path = 'data/traditional.csv'
gbq_path = 'nbar-264220.nbar_data.traditional'
df = pd.read_csv(csv_path)

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
