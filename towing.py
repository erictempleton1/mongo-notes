from pymongo import MongoClient
import json

client = MongoClient()
db = client['towing']

"""
# most towed car make
car_make = db.tows.aggregate([
	{'$group':
	    {'_id': {'Vehicle Make': '$vehicleMake'},
	     'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}},
	{'$limit': 20}
])

# most towed car color
car_color = db.tows.aggregate([
	{'$group':
	    {'_id': {'Vehicle Color': '$vehicleColor'},
	     'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}},
	{'$limit': 10}
])

# most towed plates
car_tag = db.tows.aggregate([
	{'$group':
	    {'_id': {'tagNumber': '$tagNumber'},
	     'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}},
	{'$limit': 10}
])

# most common tow locations
tow_place = db.tows.aggregate([
    {'$group':
        {'_id': {'Tow place': '$towedFromLocation'},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}},
    {'$limit': 10}
])

# average tows per day
# date includes time, so not a good representation
avg_tows = db.tows.aggregate([
	{'$group':
	    {'_id': '$towedDateTime',
	     'count': {'$sum': 1}
	    }
	},
	{'$group':
	    {'_id': '',
	     'avg tows per day': {'$avg': '$count'}
	    }
	}
])
"""

print json.dumps(avg_tows, indent=4)