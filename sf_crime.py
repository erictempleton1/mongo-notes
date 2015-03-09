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
"""

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

proj = db.crimes.aggregate([
    {'$match':
        {'DayOfWeek': 'Wednesday'}
    },
    {'$group':
        {'_id': {'Crime': '$Category'},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}},
    {'$limit': 5},
    {'$project':
        {'_id': 0,
         'Crime Category': '$_id.Crime',
         'Crime Count': '$count'
        }
    },
])

print json.dumps(proj, indent=4)

