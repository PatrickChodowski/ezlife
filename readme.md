
### Stats
Small personal library for sending quick queries to GBQ and getting back aggregates for pandas plots

Usage:

```python
from stats.gbq_data import GBQData

g = GBQData(gbq_path='project.dataset.table',
            sa_path='credentials/sa.json')

g.set(dimensions=['team_abbreviation', 'player_name'],
      metrics=['pts'],
      aggregation='count',
      sort=('pts', 'desc'),
      filters=[('team_abbreviation', 'eq', 'GUA')],
      limit=30)
g.get()
```
