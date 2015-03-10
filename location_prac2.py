from pymongo import MongoClient

client = MongoClient()
db = client['newdb']

# notes on aggregation

# returns all cities in MD with pop >= 50,000
state_50k = db.locations.aggregate([
	{'$match': {
	    'state': 'MD',
	    'pop': {'$gte': 50000}
	    }
	}
])


# returns the total population for each state
# agg creates a new dict val called totalPop
# example result:
# {u'_id': u'WA', u'totalPop': 4866692}
state_pop = db.locations.aggregate([
    {'$group':
        {'_id': '$state',
            'totalPop': {'$sum': '$pop'}
        }
    }
])

# returns states with pop >= 1000000
# creates new val called total_pop
state_million = db.locations.aggregate([
    {'$group':
        {'_id': '$state',
            'total_pop': {'$sum': '$pop'}
        }
    },
    {'$match': {
        'total_pop': {'$gte': 1000000}
        }
    }
])

# returns avg city population by state
# creates new val avg_city_pop
avg_pop = db.locations.aggregate([
	{'$group':
	    {'_id': {'state': '$state', 'city': '$city'},
	        'pop': {'$sum': '$pop'}
	    }
	},
	{'$group':
	    {'_id': '$_id.state',
	        'avg_city_pop': {'$avg': '$pop'}
	    }
	}
])

# largest and smallest cities by state
lg_sm_pop =  db.locations.aggregate([
    {'$group':
        {'_id': {'state': '$state', 'city': '$city'},
         'pop': {'$sum': '$pop'}
        }
    },
    {'$group':
        {'_id': '$_id.state',
         'biggest_city': {'$last': '$_id.city'},
         'biggest_pop': {'$last': '$pop'},
         'smallest_city': {'$first': '$_id.city'},
         'smallest_pop': {'$first': '$pop'}
        }
    }
])

# same as above but sorts desc by pop
lg_sm_pop_desc = db.locations.aggregate([
    {'$group':
        {'_id': {'state': '$state', 'city': '$city'},
         'pop': {'$sum': '$pop'}
        }
    },
    {'$sort': {'pop': 1}},
    {'$group':
        {'_id': '$_id.state',
         'biggest_city': {'$last': '$_id.city'},
         'biggest_pop': {'$last': '$pop'},
         'smallest_city': {'$first': '$_id.city'},
         'smallest_pop': {'$first': '$pop'}
        }
    }
])