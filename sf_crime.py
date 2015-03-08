# San Francisco crime dataset
# https://data.sfgov.org

from pymongo import MongoClient
from datetime import datetime
import dateutil.parser
import json

client = MongoClient()
db = client['sf_crime']

start = datetime(2013, 1, 1, 00, 00, 00)
end = datetime(2013, 12, 31, 23, 59, 00)

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
"""

years = db.crimes.aggregate([
    {'$group':
        {'_id': {'Year': {'$year': '$Date'}},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}}
])

print json.dumps(years, indent=4)

