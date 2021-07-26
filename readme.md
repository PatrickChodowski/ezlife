
### Stats
Small personal library for sending quick queries to GBQ and getting back aggregates for pandas plots

### how to install:
pip:

```commandline
pip3 install git+https://github.com/PatrickChodowski/stats.git
```

poetry:
```commandline
poetry add "git+https://github.com/PatrickChodowski/stats.git#main"
```



### Usage:

```python
from stats import GBQData

g = GBQData(gbq_path='project.dataset.table',
            sa_path='credentials/sa.json')

g.set(dimensions=['team_abbreviation', 'player_name'],
      metrics=['pts'],
      aggregations=['sum'],
      sort=('sum_pts', 'desc'),
      filters=[('team_abbreviation', 'eq', 'GUA')],
      limit=30)
g.get()
```

### Modules:

- gbq_data.py - Main interface to work with GBQ connection and sending queries
- query_builder.py - Which takes the input, validates it and builds query
- plots.py - Which takes the data from .set() method of validated query and produces optional plot

The goal of this setup is to split query building and validation from plotting, 
and have already correct data before actually visualizing the report, 
instead of learning it already when plot is created.

### Aggregation options:

- avg
- sum
- min
- max
- count
- count_distinct
- count_nulls
- any
- string_agg
- array_agg
- string_agg_distinct
- array_agg_distinct
- median
- q1
- q3
- stdev
- var

### Plot options:
- bar chart 
- horizontal bar chart
- box plot
