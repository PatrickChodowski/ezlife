from stats.gbq_data import GBQData

g = GBQData(gbq_path='nbar-264220.nbar_data.traditional',
            sa_path='credentials/nbar.json')

if __name__ == '__main__':
    g.set(dimensions=['team_abbreviation', 'player_name'],
          metrics=['pts'],
          aggregation='count',
          sort=('pts', 'desc'),
          filters=[('team_abbreviation', 'eq', 'GUA')],
          limit=30)
    g.get()
