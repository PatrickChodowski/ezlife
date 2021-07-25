

from stats.gbq_data import GBQData
from stats.utils import get_logger

logger = get_logger('stats')

gbq = GBQData(gbq_path='nbar-264220.nbar_data.traditional',
              sa_path='credentials/nbar.json',
              logger=logger)


