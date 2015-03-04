""" Top level MongoDB notes """

# start mongod and mongo in two seperate terminal tabs
mongod
mongo

# view all existing databases
show dbs

# select which db to connect to
use <database name>

# view all collections or "tables" for a db
show collections

# view stats overview for the collection
db.locations.stats()

# basic data insert
# define a var
j = {name: 'mongo'}
db.<collection name>.insert(j)

# broad search all of the collection
# returns chunked results
db.<collection name>.find()

# limit find results returned
db.<collection name>.find().limit(3)


""" Query specific notes using location (fake_data.json) db """

# find specific city
db.locations.find({city: 'HADLEY'})

# how many cities are named HADLEY?
db.locations.find({city: 'HADLEY'}).count()

# how mant cities are in the db for MD?
db.locations.find({state: 'MD'}).count()

# how many cities are in the db for TX?
db.locations.find({state: 'TX'}).count()

# return a list of all states in the db
db.locations.distinct('state')

# return a list of all cities in MD in the db
db.locations.distinct('city', {state: 'MD'})

# convert to ISODate
> while (cursor.hasNext()) {
... var doc = cursor.next();
... db.tows.update({_id: doc._id}, {$set: {towedDateTime: new Date(doc.towedDateTime)}})
... }
