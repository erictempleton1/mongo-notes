from pymongo import MongoClient

client = MongoClient()
db = client['bmore_crime']

# sum of crimes by district
# sorted desc, but includes neighborhoods
sum_district = db.crime.aggregate([
    {'$group': 
        {'_id': '$District',
            'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}}
])

# sum of crimes sorted desc.
sum_crime = db.crime.aggregate([
    {'$group':
        {'_id': '$Description',
            'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}}
])

# sum of each crime by district
dist_sum_crime = db.crime.aggregate([
	{'$group':
	    {'_id': {'District': '$District', 'Description': '$Description'},
	     'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}}
])

print db.crime.aggregate([
    {'$match':
        {'District': {'$in':
            ['NORTHEASTERN', 'WESTERN']
            }
        }
    },
    {'$group':
        {'_id': {'Description': '$Description'},
            'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}}
])



