# San Francisco crime dataset
# https://data.sfgov.org

from pymongo import MongoClient
from datetime import datetime
import dateutil.parser
import json

client = MongoClient()
db = client['sf_crime']


"""
crimes = db.crimes.aggregate([
    {'$group':
        {'_id': {'Crime': '$Category'},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}}
])

crimes_2014 = db.crimes.aggregate([
	{'$match':
	    {'Date': {'$gte': start, '$lte': end}}
	},
	{'$group':
	    {'_id': {'Crime': '$Category'},
	     'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}},
	{'$limit': 5}
])

years = db.crimes.aggregate([
    {'$group':
        {'_id': {'Year': {'$year': '$Date'}},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}}
])

start = datetime(2014, 1, 1, 00, 00, 00)
end = datetime(2014, 12, 31, 23, 59, 00)

months = db.crimes.aggregate([
    {'$match':
        {'Date': {'$gte': start, '$lte': end}}
    },
    {'$group':
        {'_id': {'Month': {'$month': '$Date'}},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}},
    {'$limit': 6}
])

# count of larceny crimes in 2014 on Sundays by month
proj = db.crimes.aggregate([
    {'$match':
        {'Date': {'$gte': start, '$lte': end},
         'DayOfWeek': 'Sunday',
         'Category': 'LARCENY/THEFT',
        }
    },
    {'$group':
        {'_id': {'Month': {'$month': '$Date'}},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}},
    {'$limit': 5},
    {'$project':
        {'_id': 0,
         'Month': '$_id.Month',
         'Count': '$count'
        }
    },
])

start = datetime(2014, 1, 1, 00, 00, 00)
end = datetime(2014, 12, 31, 23, 59, 00)

proj2 = db.crimes.aggregate([
    {'$match':
        {'Date': {'$gte': start, '$lte': end}}
    },
    {'$group':
        {'_id': {'Month': {'$month': '$Date'}},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}},
    {'$project':
        {'_id': 0,
         'Month': '$_id.Month',
         'Crime Count': '$count'
        }
    }
])

dist_crime = db.crimes.aggregate([
    {'$match':
        {'Date': {'$gte': start, '$lte': end}}
    },
    {'$group':
        {'_id': {'District': '$PdDistrict'},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}},
    {'$limit': 10},
    {'$project':
        {'_id': 0,
         'District': '$_id.District',
         'Crime Count': '$count'
        }
    }
])

per_day = db.crimes.aggregate([
    {'$match':
        {'Date': {'$gte': start, '$lte': end}}
    },
    {'$group':
        {'_id': {'Day of Year': {'Date': {'$dayOfYear': '$Date'}}},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}},
    {'$limit': 10}
])

avg_crimes = db.crimes.aggregate([
    {'$match':
        {'Date': {'$gte': start, '$lte': end}}
    },
    {'$group':
        {'_id': {'Day of Year': {'Date': {'$dayOfYear': '$Date'}}},
         'count': {'$sum': 1}
        }
    },
    {'$group': 
        {'_id': '',
            'Avg Per Day': {'$avg': '$count'}
        }
    }
])

# match by date plus project extras
total_year = db.crimes.aggregate([
    {'$match':
        {'Date': {'$gte': start, '$lte': end}}
    },
    {'$group':
        {'_id': {'Year': {'$year': '$Date'}},
         'Count': {'$sum': 1},
        }
    },
    {'$project':
        {'_id': 0,
            'Year': '$_id.Year',
            'Sum Per Year': '$Count',
            'Avg Per Day': {'$divide': ['$Count', 365]},
            'Avg Per Month': {'$divide': ['$Count', 12]},
        }
    },
    {'$sort': {'Sum Per Year': -1}}
])

start = datetime(2003, 1, 1, 00, 00, 00)
end = datetime(2014, 12, 31, 23, 59, 00)

# sort months desc by most crimes
large_month = db.crimes.aggregate([
    {'$match':
        {'Date': {'$gte': start, '$lte': end}}
    },
    {'$group':
        {'_id': {'Year': {'$year': '$Date'}, 'Month': {'$month': '$Date'}},
         'Count': {'$sum': 1},
        }
    },
    {'$sort': {'Count': -1}},
    {'$limit': 10},
    {'$project':
        {'_id': 0,
            'Year': '$_id.Year',
            'Month': '$_id.Month',
            'Crimes In Given Month': '$Count'
        }
    }
])
"""

start = datetime(2003, 1, 1, 00, 00, 00)
end = datetime(2014, 12, 31, 23, 59, 00)

day_week = db.crimes.aggregate([
    {'$match':
        {'Date': {'$gte': start, '$lte': end}}
    },
    {'$group':
        {'_id': {'Year': {'$year': '$Date'}, 'Day of Week': '$DayOfWeek'},
         'count': {'$sum': 1}
        }
    },
    {'$limit': 5},
    {'$sort': {'count': -1}}
])

print json.dumps(day_week, indent=4)

