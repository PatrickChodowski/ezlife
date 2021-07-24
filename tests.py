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
                aggregation='sum',
                sort='desc',
                filters=[('team_abbreviation', 'in', ['DEN','WAS','HOU','LAL'])],
                logger=logger
                )
pp.bar()


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

