import unittest
import pandas as pd
from stats import GBQData


class GBQDataPlotTests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(GBQDataPlotTests, self).__init__(*args, **kwargs)
        self.g = GBQData(gbq_path='nbar-264220.nbar_data.traditional',
                         sa_path='../credentials/nbar.json')

    def test_plot_bar_single_dim_metric(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregation='median',
                   sort=('pts', 'desc'),
                   filters=[('team_abbreviation', 'in', ['DEN', 'LAL', 'LAC', 'MIL', 'UTA'])])
        df = self.g.get(plot_type='bar')
        assert isinstance(self.g.get('bar'), pd.DataFrame)
