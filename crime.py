# Baltimore crime data set
# https://data.baltimorecity.gov/Public-Safety/BPD-Part-1-Victim-Based-Crime-Data/wsfq-mvij

from pymongo import MongoClient
import json

client = MongoClient()
db = client['bmore_crime']

"""
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


# returns avg crime per day per district
# limits to top 10 districts
avg_per_district = db.crime.aggregate([
    {'$group':
        {'_id': {'CrimeDate': '$CrimeDate', 'District': '$District'},
         'count': {'$sum': 1}
        }
    },
    {'$group':
        {'_id': '$_id.District',
         'avg crimes per day': {'$avg': '$count'}
        }
    },
    {'$sort': {'avg crimes per day': -1}},
    {'$limit': 10}
])

# returns avg per location
# data contains many duplicates which skews results
# the below lead me to find the duplicates
avg_per_location = db.crime.aggregate([
	{'$group':
	    {'_id': {'Location': '$Location', 'CrimeDate': '$CrimeDate'},
	     'count': {'$sum': 1}
	    }
	},
	{'$group':
	    {'_id': '$_id.Location',
	     'avg crimes per day': {'$avg': '$count'}
	    }
	},
	{'$sort': {'avg crimes per day': -1}},
	{'$limit': 10}
])

total_per_location = db.crime.aggregate([
    {'$group':
        {'_id': '$Location',
         'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}},
    {'$limit': 10}
])

avg_per_hotel = db.crime.aggregate([
    {'$match':
        {'Location': '900 N BRUCE ST'}
    },
    {'$group':
        {'_id': '$CrimeDate',
         'count': {'$sum': 1}
        }
    },
    {'$group':
        {'_id': '',
         'sum': {'$sum': '$count'},
         'avg': {'$avg': '$count'}
        }
    }
])

avg_high = db.crime.aggregate([
	{'$match':
	    {'Location': '900 N BRUCE ST'}
	},
	{'$group':
	    {'_id': '$CrimeDate',
	     'count': {'$sum': 1}
	    }
	}
])
"""

# sub in var for nice format json print
print json.dumps(avg_high, indent=4)