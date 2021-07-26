import unittest
import pandas as pd
from stats.gbq_data import GBQData


class GBQDataGetTests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(GBQDataGetTests, self).__init__(*args, **kwargs)
        self.g = GBQData(gbq_path='nbar-264220.nbar_data.traditional',
                         sa_path='../credentials/nbar.json')

    def test_connection(self):
        r = self.g.gbq.get_data('SELECT 1 as test_value')['test_value'][0]
        assert r == 1

    def test_single_group_average(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregation='avg',
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multidimension_group_average(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts'],
                   aggregation='avg',
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_group_average(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts', 'fga'],
                   aggregation='avg',
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_average(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='avg',
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_sum(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='sum',
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_min(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='min',
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_max(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='max',
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_dim(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='max',
                   sort=('team_abbreviation', 'desc'),
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='max',
                   sort=('pts', 'desc'),
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_limit(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='max',
                   sort=('pts', 'desc'),
                   filters=None,
                   limit=10)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_filter_in(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='max',
                   sort=('pts', 'desc'),
                   filters=[('team_abbreviation', 'in', ['DEN', 'LAL', 'MIL', 'PHX', 'UTA', 'BOS'])],
                   limit=10)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_filter_nin(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='max',
                   sort=('pts', 'desc'),
                   filters=[('team_abbreviation', 'nin', ['DEN', 'LAL', 'MIL', 'PHX', 'UTA', 'BOS'])],
                   limit=10)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_filter_eq(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='max',
                   sort=('pts', 'desc'),
                   filters=[('team_abbreviation', 'eq', 'DEN')],
                   limit=10)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_filter_ne(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='max',
                   sort=('pts', 'desc'),
                   filters=[('team_abbreviation', 'ne', 'DEN')],
                   limit=10)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_filter_ge(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregation='max',
                   sort=('pts', 'desc'),
                   filters=[('fga', 'ge', 1)],
                   limit=10)
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_simple_group_max_double_filter(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregation='avg',
                   filters=[('fga', 'ge', 1),
                            ('pts', 'lt', 40)])
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_simple_group_max_triple_filter(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregation='avg',
                   filters=[('fga', 'ge', 1),
                            ('pts', 'lt', 40),
                            ('mins', 'ge', 1)
                            ])
        assert isinstance(self.g.get(''), pd.DataFrame)

    def test_simple_max_no_dimension(self):
        self.g.set(dimensions=None,
                   metrics=['pts'],
                   aggregation='avg')
        assert isinstance(self.g.get(''), pd.DataFrame)


