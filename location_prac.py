from pymongo import MongoClient
#import re

client = MongoClient()

# connect to db named newdb
db = client['newdb']

# return total number of records in the locations collection
print '{0} records in the locations collection'.format(db.locations.find().count())

# total cities in MD and PA
# $in requires regex in quotes
md_pa_count = db.locations.find({'state': {'$in': ['MD','PA']} }).count()
print '{0} cities in MD and PA'.format(md_pa_count)

# find all MD cities with pop > 50,000
md_50k = db.locations.find({'state': 'MD', 'pop': {'$gt': 50000} }).count()
print '{0} cities in MD with a population greater than 50,000'.format(md_50k)

# count all cities with pop > 50,000
all_50k = db.locations.find({'pop': {'$gt': 50000} }).count()
print '{0} cities in America with population greater than 50,000'.format(all_50k)

# count all cities with pop <= 500
all_1k = db.locations.find({'pop': {'$lte': 500} }).count()
print '{0} cities in America with population less than or equal to 500'.format(all_1k)

# return the top cities with highest pop
high_pop = db.locations.find().sort('pop', -1).limit(5)
print 'Top 5 cities with highest population:'
for city in high_pop:
	print city

# cities in MD with population less than or equal to 100
md_100 = db.locations.find({'state': 'MD', 'pop': {'$lte': 100} }).sort('pop', -1)
print 'Cities in MD with population less than or equal to 100:'
for city in md_100:
	print city

# $and operator for 100 to 500 pop cities in MD
md_100_500 = db.locations.find(
	{
    'state': 'MD',
    '$and':
        [
         {'pop': {'$gte': 100}},
         {'pop': {'$lte': 500}}
        ] 
    })
# prints count of cities with 100 to 500 pop
print '{0} cities in MD with a population of 100 to 500'.format(md_100_500.count())

# prints first 5 cities descending by pop size
for city in md_100_500.sort('pop', -1).limit(5):
	print city

