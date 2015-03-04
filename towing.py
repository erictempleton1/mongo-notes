from pymongo import MongoClient
from datetime import datetime
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

# tows per year
years = db.tows.aggregate([
	{'$group':
	    {'_id': {'year': {'$year': '$towedDateTime'}},
	     'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}}
])

# tows per month sorted desc
months = db.tows.aggregate([
	{'$group':
	    {'_id': {'month': {'$month': '$towedDateTime'}},
	     'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}}
])


# tows per day of the month sorted desc
days = db.tows.aggregate([
	{'$group':
	    {'_id': {'day': {'$dayOfMonth': '$towedDateTime'}},
	     'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}}
])

# tows per day of the week sorted desc
day_of_week = db.tows.aggregate([
    {'$group':
        {'_id': {'day of week': {'$dayOfWeek': '$towedDateTime'}},
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}}
])
"""

# uses start date to find count of all tows in 2013 & 2014
start = datetime(2013, 1, 1, 00, 00, 00)
end = datetime(2014, 12, 31, 23, 59, 00)
tows_year = db.tows.aggregate([
	{'$match': 
	    {'towedDateTime': {'$gte': start, '$lte': end}}
	},
	{'$group':
	    {'_id': {'year': {'$year': '$towedDateTime'}},
	     'count': {'$sum': 1}
	    }
	}
])

print json.dumps(tows_year, indent=4)