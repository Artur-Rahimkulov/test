const { readFileSync } = require('fs')
const { MongoClient } = require('mongodb')

const client = new MongoClient('mongodb://localhost:27017')
const start = async () => {
    try {
        await client.connect()
        console.log('connected')
        let firstCollection = await client.db().collection('first')
        let secondCollection = await client.db().collection('second')
        await firstCollection.deleteMany({})
        await secondCollection.deleteMany({})
        await firstCollection.insertMany(await JSON.parse(readFileSync('first.json')))
        await secondCollection.insertMany(await JSON.parse(readFileSync('second.json')))
        const agg = [
            {
              '$addFields': {
                'longitude': {
                  '$first': '$location.ll'
                }, 
                'latitude': {
                  '$last': '$location.ll'
                }
              }
            }, {
              '$lookup': {
                'from': 'second', 
                'localField': 'country', 
                'foreignField': 'country', 
                'as': 'second'
              }
            }, {
              '$addFields': {
                '_id': '$country', 
                'allDiffs': {
                  '$subtract': [
                    {
                      '$getField': {
                        'input': {
                          '$first': {
                            '$sortArray': {
                              'input': '$students', 
                              'sortBy': {
                                'year': -1
                              }
                            }
                          }
                        }, 
                        'field': 'number'
                      }
                    }, {
                      '$getField': {
                        'input': {
                          '$first': '$second'
                        }, 
                        'field': 'overallStudents'
                      }
                    }
                  ]
                }
              }
            }, {
              '$group': {
                '_id': '$_id', 
                'count': {
                  '$count': {}
                }, 
                'allDiffs': {
                  '$push': '$allDiffs'
                }, 
                'longitude': {
                  '$push': '$longitude'
                }, 
                'latitude': {
                  '$push': '$latitude'
                }
              }
            }, {
              '$out': 'third'
            }
          ];
          const cursor = firstCollection.aggregate(agg);
          const result = await cursor.toArray();
          await client.close();
        console.log("completed")
    }
    catch (e) {
        console.log(e)
    }
}
start()