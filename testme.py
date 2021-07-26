

from stats.gbq_data import GBQData
from stats.utils import get_logger

logger = get_logger('stats')

if __name__ == '__main__':
    g = GBQData(gbq_path='nbar-264220.nbar_data.traditional',
                  sa_path='credentials/nbar.json',
                  logger=logger)

    # will specify data
    # g.set(dimensions = , metrics = , filters = , sort = )

    g.get('bar')