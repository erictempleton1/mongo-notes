# Baltimore crime data set
# https://data.baltimorecity.gov/Public-Safety/BPD-Part-1-Victim-Based-Crime-Data/wsfq-mvij

from pymongo import MongoClient
import json

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

# returns crime sums from districts in the list
crime_sum_dist = db.crime.aggregate([
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

# top 20 highest crime days sorted desc
crime_date_20 = db.crime.aggregate([
	{'$group':
	    {'_id': '$CrimeDate',
	        'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}},
	{'$limit': 20}
])

# returns highest and lowest crime dates
crime_date_high_low = db.crime.aggregate([
	{'$group':
	    {'_id': {'CrimeDate': '$CrimeDate'},
	        'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}},
	{'$group':
	    {'_id': 'CrimeDate',
	     'higest_date': {'$first': '$_id.CrimeDate'},
	     'highest_count': {'$first': '$count'},
	     'lowest_date': {'$last': '$_id.CrimeDate'},
	     'lowest_count': {'$last': '$count'}
	    }
	}
])

# returns avg number of crimes per day
avg_day = db.crime.aggregate([
	{'$group':
	    {'_id': '$CrimeDate',
	     'count': {'$sum': 1}
	    }
	},
	{'$group':
	    {'_id': '',
	     'avg_crime': {'$avg': '$count'}
	    }
	}
])

# returns count of each weapon used sorted desc
weapon_count = db.crime.aggregate([
    {'$group':
        {'_id': '$Weapon',
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}},
])

# returns sorted desc count of common weapons used in given crime
crime_weapon = db.crime.aggregate([
	{'$match':
	    {'Description': 'ROBBERY - STREET'}
	},
	{'$group':
	    {'_id': {'Weapon': '$Weapon'},
	        'count': {'$sum': 1}
	    }
	},
	{'$sort': {'count': -1}},
])

# average crimes per day with a firearm
avg_firearm = db.crime.aggregate([
	{'$match':
	    {'Weapon': 'FIREARM'}
	},
	{'$group':
	    {'_id': '$CrimeDate',
	     'count': {'$sum': 1}
	    }
	},
	{'$group':
	    {'_id': '',
	     'avg per day': {'$avg': '$count'}
	    }
	}
])

# average crimes per day all weapons
avg_all_weapons = db.crime.aggregate([
    {'$group':
        {'_id': {'CrimeDate': '$CrimeDate', 'Weapon': '$Weapon'},
         'count': {'$sum': 1}
        }
    },
    {'$group':
        {'_id': '$_id.Weapon',
         'avg per day': {'$avg': '$count'}
        }
    }
])

# sub in var for nice format json print
print json.dumps(avg_all_weapons, indent=4)