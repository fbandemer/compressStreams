var AWS = require('aws-sdk');
AWS.config.region = 'eu-central-1';
var lambda = new AWS.Lambda();
const ddb = new AWS.DynamoDB.DocumentClient({region: 'eu-central-1'})
const s3 = new AWS.S3();

const s3Bucket = 'streamerdata'; 
const currentStreamsKey = 'updateData/currentActiveStreamers';
const lastStreamsKey = 'updateData/lastActiveStreamers'; 

var readPromises = []
var writePromises = []
var writes = []
var beforeCompress = 0
var afterCompress = 0
var fetchActive = true
var trueWrites = 0
var endpurt = 0
var evaluateEndspurt = false

const updateDiff = async() => {
  readPromises = []
  writePromises = []
  writes = []
  beforeCompress = 0
  afterCompress = 0
  trueWrites = 0
  endpurt = Math.floor(Date.now() / 1000) + 500
  fetchActive = true
  evaluateEndspurt = false
  writePromises.push(writeData())
  let finishedIDs = await getIDS()
  readPromises.push(readData(finishedIDs))
  let xx = await Promise.all(readPromises)
  await delay(10000)
  fetchActive = false
  evaluateEndspurt = true
  let y = await Promise.all(writePromises)
  console.log(writePromises)
  console.log('compression: ' + (1 - (afterCompress / beforeCompress)))
  console.log('writes: ' + trueWrites + ' vs. writes: ' + writes.length)

  await delay(5000)
  console.log('writes: ' + trueWrites + ' vs. writes: ' + writes.length)
    const response = {
        statusCode: 200,
        body: JSON.stringify('Hello from Lambda!'),
    };
    return response;
};


function getData(userID,tableName) {
    let pastTimestamp = Math.floor(Date.now() / 1000) - 86400
    var params = {
    TableName: tableName,
    ConsistentRead: false,
    KeyConditionExpression: "PK = :v1 AND SK >= :v2",
    ExpressionAttributeValues: {
        ':v1': 'user_' + userID,
        ':v2': '' + pastTimestamp
    },
    ReturnConsumedCapacity: "TOTAL"
    }
    return ddb.query(params).promise().catch(function(err) {
        console.log('db reas error')
        console.log(params)
        console.log(err);
      });
}

function getLastStreams() {
    let params = {
        Bucket: s3Bucket,
        Key: lastStreamsKey,
     };
     return s3.getObject(params).promise()
}

function getCurrentStreams() {
    let params = {
        Bucket: s3Bucket,
        Key: currentStreamsKey,
     };
     return s3.getObject(params).promise()
}



function handleItem(userID) {
  return new Promise(async resolve => {
    let currentData = await getData(userID,"userStreams")
    let oldData = await getData(userID,"compressedStreams")
    lastTimeStamp = 0
    if (currentData.Items.length > 0) {
      beforeCompress = beforeCompress + Buffer.byteLength(JSON.stringify(currentData))
      let newStreams = currentData.Items//.slice(0,6) // new Streams
      let oldStreams = oldData.Items
      let dataAdded = []
      let uniqueStreams = []
      for (stream of newStreams) {
        let match = false
        if (!match) {
          for(i=0; i < oldStreams.length; i++) {
            if (!match) {
              for(j=0; j<oldStreams[i].data.length; j++) {
                if (!match) {
                  if (oldStreams[i].data[j].id == stream.id) {
                    let timestamp = parseInt(stream.SK.replace('_' + stream.game_id, ''))
                    if (!match) {
                      for(n=0;n<oldStreams[i].data[j].data.length;n++) {
                        if (!match) {
                          if(parseInt(oldStreams[i].data[j].data[n].timestamp) == timestamp) {
                            match = true
                          }
                          if (parseInt(oldStreams[i].data[j].data[n].timestamp) < timestamp) {
                            let details = {}
                            details.viewer_count = stream.viewer_count
                            details.timestamp = parseInt(timestamp)
                            details.game_id = stream.game_id
                            oldStreams[i].data[j].data.push(details)
                            match = true
                            if (!dataAdded.includes(i)) {
                              dataAdded.push(i)
                            }
                          }
                        }
                      }
                    }
                }
                }
              }
            }
          }
        }
        if (!match) {
          uniqueStreams.push(stream)
        }
      }
      for (index of dataAdded) {
        let params = {
          TableName: 'testec2',
          Item: oldStreams[index]
        }
        writes.push(params)
      }
      if (uniqueStreams.length > 0) {
        let userData = {}
        let streamMeta = {}
        let streamIds = []
        let streamData = []
        let streamsArray = []
        userData.PK = uniqueStreams[0].PK
        userData.SK = uniqueStreams[0].SK.replace('_' + uniqueStreams[0].game_id, '')
        userData.ttl = Math.floor(Date.now() / 1000) + 5184000
        streamMeta.title = uniqueStreams[0].title
        streamMeta.language = uniqueStreams[0].language
        streamMeta.id = uniqueStreams[0].id
        let details = {}
        streamIds.push(uniqueStreams[0].id)
        for (entry of uniqueStreams) {
          details.viewer_count = entry.viewer_count
          details.timestamp = parseInt(entry.SK.replace('_' + entry.game_id, ''))
          details.game_id = entry.game_id
          if (streamIds.includes(entry.id)) {
            streamData.push(details)
            details = {}
          } else {
            streamMeta.data = streamData
            streamsArray.push(streamMeta)
            streamData = []
            streamMeta = {}
            streamMeta.title = entry.title
            streamMeta.language = entry.language
            streamMeta.id = entry.id
            streamData.push(details)
            details = {}
            streamIds.push(entry.id)
          }
        }
        streamMeta.data = streamData
        streamsArray.push(streamMeta)
        userData.data = streamsArray
        let params = {
          TableName: 'testec2',
          Item: userData
        }
        writes.push(params)
        afterCompress = afterCompress + Buffer.byteLength(JSON.stringify(userData))
      }
    }
    resolve(true)
  })
}


async function getIDS() {
  let currentData = await getCurrentStreams()
  let data = await getLastStreams()
  let currentUserIds = JSON.parse(currentData.Body.toString())
  let oldUserIds = JSON.parse(data.Body.toString())
  console.log(currentData)
  console.log(currentUserIds)
  let oldIdsInDb = new Map()
  let idsInDb = new Map()
  let finishedStreams = []; 
  let newStreams = [];
  for (entry of currentUserIds) {
      idsInDb.set(entry, entry)
  }
  oldUserIds.forEach((oldId) => {
    oldIdsInDb.set(oldId, oldId)
    let id = idsInDb.get(oldId)
    if (id) {
    } else {
      finishedStreams.push(oldId)
    }
  });
  currentUserIds.forEach((newID) => {
    const id = oldIdsInDb.get(newID)
    if (id) {
    } else {
      newStreams.push(newID)
    }
  })
  console.log('new Streams: ' + newStreams.length + ' and finishedStreams: ' + finishedStreams.length)
  //await sendStarted(newStreams)
  //await sendStreams(currentUserIds)
  //await sendCurrentDiff(finishedStreams)
  return finishedStreams
}


async function writeData() {
  let writeCounter = 0
  let lastRun = 0
  let idle = false
  let normalRate = 85
  do {
    if (writes.length > 0) {
      try {
        writePromises.push(dbWrite(writes[0]))
        trueWrites++
      } catch (err) {
          console.log("Failure", err.message)
      }
      writes.shift()
/*      if (idle) {
        let now = Math.floor(Date.now() / 1000)
        console.log('Last idle: ' + (now - lastRun) + 'secs')
        idle = false
        lastRun = 0
      }*/
    } else {
/*      if (lastRun == 0) {
        lastRun = Math.floor(Date.now() / 1000)
        console.log('lastRun started ' + lastRun)
        idle = true
      }*/
    }
    writeCounter++
    if (writeCounter >= normalRate) {
      //console.log('beforeWait with write:' + writeCounter)
      await delay(1000)
      //console.log('afterWait')
      writeCounter = 0
      let now = Math.floor(Date.now() / 1000)
    }
  } while(fetchActive || (writes.length > 0));
  console.log('finished writes')
  return 'durch'
}

async function readData(reads) {
  let readCounter = 0
  let lastRun = 0
  let idle = false
  let lastReads = 0
  let normalRate = 150
  do {
    if (reads.length > 0) {
      try {
        readPromises.push(handleItem(reads[0]))
      } catch (err) {
          console.log("Failure", err.message)
      }
      reads.shift()
      if (idle) {
        let now = Math.floor(Date.now() / 1000)
        console.log('Last read idle: ' + (now - lastRun) + 'secs')
        idle = false
        lastRun = 0
      }
    } else {
      if (lastRun == 0) {
        lastRun = Math.floor(Date.now() / 1000)
        console.log('lastRun started ' + lastRun)
        idle = true
      }
    }
    readCounter++
    if (readCounter >= normalRate) {
      lastReads = trueWrites
      await delay(1000)
      readCounter = 0
    }
  } while(reads.length > 0);
  console.log('finished reads')
  return 'durch'
}

function dbWrite(params) {
  return new Promise(async resolve => {
      ddb.put(params).promise().then(function(data) {
        resolve(true)
      }).catch(function(err) {
        console.log('db write error')
        console.log(params)
        console.log(err);
        resolve(false)
      });
      
  })
}

async function sendStreams(userIds) {
    let send = JSON.stringify(userIds)
    try {
        let params = {
           Bucket: s3Bucket,
           Key: lastStreamsKey,
           Body: send,
        };
        await s3.putObject(params).promise();
        console.log(`File uploaded successfully at https:/` + s3Bucket +   `.s3.amazonaws.com/` + lastStreamsKey);
        return true
      } catch (error) {
          console.log(error)
        console.log('error');
        return false
      }
}

async function sendCurrentDiff(userIds) {
    let send = JSON.stringify(userIds)
    try {
        let params = {
           Bucket: s3Bucket,
           Key: 'updateData/endedStreams',
           Body: send,
        };
        await s3.putObject(params).promise();
        console.log(`File uploaded successfully at https:/` + s3Bucket +   `.s3.amazonaws.com/updateData/endedStreams`);
        return true
      } catch (error) {
          console.log(error)
        console.log('error');
        return false
      }
}

async function sendStarted(userIds) {
    let send = JSON.stringify(userIds)
    try {
        let params = {
           Bucket: s3Bucket,
           Key: 'updateData/startedStreams',
           Body: send,
        };
        await s3.putObject(params).promise();
        console.log(`File uploaded successfully at https:/` + s3Bucket +   `.s3.amazonaws.com/updateData/startedStreams`);
        return true
      } catch (error) {
          console.log(error)
        console.log('error');
        return false
      }
}

function delay(milisec) {
    return new Promise(resolve => {
        setTimeout(() => { resolve('') }, milisec);
    })
}

(async () => {
    const diffResult = await updateDiff();
    console.log(diffResult)
    process.exit()
  })();
