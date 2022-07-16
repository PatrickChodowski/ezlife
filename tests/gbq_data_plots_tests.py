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
                   aggregations=['median'],
                   sort=('median_pts', 'desc'),
                   filters=[('team_abbreviation', 'in', ['DEN', 'LAL', 'LAC', 'MIL', 'UTA'])])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_plot_bar_multi_dim_single_metric(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts'],
                   aggregations=['median'],
                   sort=('median_pts', 'desc'),
                   filters=[('team_abbreviation', 'in', ['DEN', 'LAL', 'LAC', 'MIL', 'UTA'])])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_plot_bar_multi_dim_multi_metric(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['median'],
                   sort=('median_pts', 'desc'),
                   filters=[('team_abbreviation', 'in', ['DEN', 'LAL', 'LAC', 'MIL', 'UTA'])])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_plot_box_multi_dim_multi_metric(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['q1', 'median', 'q3', 'min_run', 'max_run'],
                   sort=('median_pts', 'desc'),
                   filters=[('team_abbreviation', 'in', ['DEN', 'LAL', 'LAC', 'MIL', 'UTA'])])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_plot_box_multi_dim_single_metric(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts'],
                   aggregations=['q1', 'median', 'q3', 'min_run', 'max_run'],
                   sort=('median_pts', 'desc'),
                   filters=[('team_abbreviation', 'in', ['DEN', 'LAL', 'LAC', 'MIL', 'UTA'])])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_plot_box_single_dim_single_metric(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregations=['q1', 'median', 'q3', 'min_run', 'max_run'],
                   sort=('median_pts', 'desc'),
                   filters=[('team_abbreviation', 'in', ['DEN', 'LAL', 'LAC', 'MIL', 'UTA'])])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_plot_box_single_dim_single_metric_no_sort(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregations=['q1', 'median', 'q3', 'min_run', 'max_run'],
                   filters=[('team_abbreviation', 'in', ['DEN', 'LAL', 'LAC', 'MIL', 'UTA'])])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_scatter_plot_no_dim(self):
        self.g.set(metrics=['pts'],
                   aggregations=['sum', 'avg'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_scatter_plot_dim(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregations=['sum', 'avg'])
        assert isinstance(self.g.get(), pd.DataFrame)
