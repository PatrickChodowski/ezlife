import unittest
import pandas as pd
from stats import GBQData


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
                   aggregations=['avg'],
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multidimension_group_average(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts'],
                   aggregations=['avg'],
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_group_average(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts', 'fga'],
                   aggregations=['avg'],
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_average(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['avg'],
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_sum(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['sum'],
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_min(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['min'],
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_max(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['max'],
                   sort=None,
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_dim(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['max'],
                   sort=('team_abbreviation', 'desc'),
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['max'],
                   sort=('max_pts', 'desc'),
                   filters=None,
                   limit=None)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_limit(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['max'],
                   sort=('max_pts', 'desc'),
                   filters=None,
                   limit=10)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_filter_in(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['max'],
                   sort=('max_pts', 'desc'),
                   filters=[('team_abbreviation', 'in', ['DEN', 'LAL', 'MIL', 'PHX', 'UTA', 'BOS'])],
                   limit=10)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_filter_nin(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['max'],
                   sort=('max_pts', 'desc'),
                   filters=[('team_abbreviation', 'nin', ['DEN', 'LAL', 'MIL', 'PHX', 'UTA', 'BOS'])],
                   limit=10)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_filter_eq(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['max'],
                   sort=('max_pts', 'desc'),
                   filters=[('team_abbreviation', 'eq', 'DEN')],
                   limit=10)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_filter_ne(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['max'],
                   sort=('max_pts', 'desc'),
                   filters=[('team_abbreviation', 'ne', 'DEN')],
                   limit=10)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_multivalue_multidimension_group_max_sort_metric_filter_ge(self):
        self.g.set(dimensions=['team_abbreviation', 'player_name'],
                   metrics=['pts', 'fga'],
                   aggregations=['max'],
                   sort=('max_pts', 'desc'),
                   filters=[('fga', 'ge', 1)],
                   limit=10)
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_group_max_double_filter(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregations=['avg'],
                   filters=[('fga', 'ge', 1),
                            ('pts', 'lt', 40)])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_group_max_triple_filter(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregations=['avg'],
                   filters=[('fga', 'ge', 1),
                            ('pts', 'lt', 40),
                            ('mins', 'ge', 1)
                            ])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_max_no_dimension(self):
        self.g.set(dimensions=None,
                   metrics=['pts'],
                   aggregations=['avg'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_count_distinct_no_dimension(self):
        self.g.set(dimensions=None,
                   metrics=['player_name'],
                   aggregations=['count_distinct'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_count_distinct_dimension(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['player_name'],
                   aggregations=['count_distinct'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_count_nulls_no_dimension(self):
        self.g.set(dimensions=None,
                   metrics=['comment'],
                   aggregations=['count_nulls'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_count_nulls_dimension(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['comment'],
                   aggregations=['count_nulls'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_array_agg_dimension(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['player_name'],
                   aggregations=['array_agg'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_array_agg_distinct_dimension(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['player_name'],
                   aggregations=['array_agg_distinct'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_array_agg_no_dimension(self):
        self.g.set(dimensions=None,
                   metrics=['player_name'],
                   aggregations=['array_agg'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_array_agg_distinct_no_dimension(self):
        self.g.set(dimensions=None,
                   metrics=['player_name'],
                   aggregations=['array_agg_distinct'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_string_agg_dimension(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['player_name'],
                   aggregations=['string_agg'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_string_agg_distinct_dimension(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['player_name'],
                   aggregations=['string_agg_distinct'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_median_dimension(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregations=['median'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_median_no_dimension(self):
        self.g.set(dimensions=None,
                   metrics=['pts'],
                   aggregations=['median'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_q3_dimension(self):
        self.g.set(dimensions=['team_abbreviation'],
                   metrics=['pts'],
                   aggregations=['q3'])
        assert isinstance(self.g.get(), pd.DataFrame)

    def test_simple_q3_no_dimension(self):
        self.g.set(dimensions=None,
                   metrics=['pts'],
                   aggregations=['q3'])
        assert isinstance(self.g.get(), pd.DataFrame)
