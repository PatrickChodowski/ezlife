

from stats.gbq_data import GBQData
from stats.utils import get_logger

logger = get_logger('stats')
g = GBQData(gbq_path='nbar-264220.nbar_data.traditional',
            sa_path='credentials/nbar.json',
            logger=logger)

if __name__ == '__main__':
    g.set(dimensions=['team_abbreviation'],
          metrics=['pts'],
          aggregation='sum',
          sort=,
          filters=None)
    g.get('')
