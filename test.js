var level = require('level-hyper')
var net = require('net')
var Replicator = require('./index')
var rmrf = require('rimraf')
var tap = require('tap')
var test = tap.test


function createOpts(localport, ports, min, failAfter) {
  var peers = ports.map(function(port) {
    return { host: 'localhost', port: port }
  })

  return { 
    peers: peers,
    port: localport,
    minConcensus: min,
    failAfter: failAfter || 16,
    host: 'localhost'
  }
}


function createData(prefix, size) {

  var map = function() { 
    return {
      key: prefix + Math.random().toString(15).slice(-256),
      value: Math.random().toString(15)
    }
  }

  var sort = function (a, b) {
    if (a.key > b.key) {
      return 1
    }
    if (a.key < b.key) {
      return -1
    }
    return 0
  }

  return Array.apply(null, new Array(size)).map(map).sort(sort)
}


test('more than two peers', function(t) {

  var i = 8
  var dbs = {}
  var rs = {}
  var servers = {}

  while(i > 0) {
    rmrf.sync('./db' + i)
    dbs['db' + i] = level('./db' + i, { valueEncoding: 'json' })
    i--
  }

  //
  // create three databases and start them.
  //
  test('setup', function(t) {

    rs.r1 = Replicator(dbs.db1, createOpts(3001, [3002, 3003]))
    rs.r2 = Replicator(dbs.db2, createOpts(3002, [3003, 3001]))
    rs.r3 = Replicator(dbs.db3, createOpts(3003, [3001, 3002]))

    servers.r1 = net.createServer(function(con) {
      var server = rs.r1.createServer()
      server.pipe(con).pipe(server)
    }).listen(3001)
    
    servers.r2 = net.createServer(function(con) {
      var server = rs.r2.createServer()
      server.pipe(con).pipe(server)
    }).listen(3002)

    servers.r3 = net.createServer(function(con) {
      var server = rs.r3.createServer()
      server.pipe(con).pipe(server)
    }).listen(3003)

    //
    // peers should be ready for replication right away because
    // writes that are made before all connections are established
    // are queued.
    //
    t.end()
  })



  test('that a random number of records put to one peer are replicated to all other peers', function(t) {

    var size = 2
    var index = 0
    var records = createData('A_', size)

    records.forEach(function(record) {

      dbs.db1.put(record.key, record.value, function(err) {
        t.ok(!err)

        //
        // after the last record is put, all records should be in the database.
        //

        if (++index == records.length) {
          [dbs.db1, dbs.db2, dbs.db3].forEach(verifyRecords)
        }
      })
    })

    function verifyRecords(db, index) {

      var results = []
      var count = 0

      db.createReadStream({ gte: 'A_', lte: 'A_~' })
        .on('data', function(r) {
          ++count
          results.push(r)
        })
        .on('end', function() {
          t.equal(count, records.length)
          t.equal(
            JSON.stringify(results), JSON.stringify(records)
          )
          if (index == 1) return t.end() 
        })
    }
  })



  test('that records batched to one peer are replicated to all other peers', function(t) {

    function chunk(arr, n) {
      return !arr.length ? [] : [arr.slice(0, n)].concat(chunk(arr.slice(n), n))
    }

    var size = 10
    var records = createData('B_', size)
    var groups = chunk(records, 2)

    groups.forEach(function(group, index) {
      var newgroup = []
      group.forEach(function(op) {
        newgroup.push({ type: 'put', key: op.key, value: op.value })
      })

      dbs.db1.batch(newgroup, function(err) {
        t.ok(!err)
        if (index == groups.length-1) {
          [dbs.db2, dbs.db3].forEach(verifyRecords)
        }
      })
    })

    function verifyRecords(db, index) {
      var results = []
      db.createReadStream({ gte: 'B_', lte: 'B_~' })
        .on('data', function(r) {
          results.push(r)
        })
        .on('end', function() {
          t.equal(
            JSON.stringify(results), JSON.stringify(records)
          )
          if (index == 1) return t.end() 
        })
    }
  })



  test('that a single record is added to all peers before returning the callback', function(t) {
    
    dbs.db1.put('test1key', 'test1value', function(err) {
      t.ok(!err, 'key added to the coordinator and all other peers')
      dbs.db2.get('test1key', function(err) {
        t.ok(!err, 'key was found in dbs.db2')
        dbs.db3.get('test1key', function(err) {
          t.ok(!err, 'key was found in dbs.db3')
          t.end()
        })
      })
    })
  })



  test('that a record can be deleted from one database and it is removed from all others', function(t) {
    
    dbs.db1.del('test1key', function(err) {
      t.ok(!err, 'key added to the coordinator and all other peers')
      dbs.db2.get('test1key', function(err) {
        t.ok(err, 'key was not found in dbs.db2')
        dbs.db3.get('test1key', function(err) {
          t.ok(err, 'key was not found in dbs.db3')
          t.end()
        })
      })
    })
  })



  test('writes should not be made if the minimum number of required peers for concensus can not be reached', function(t) {

    //
    // only retry 4 times in the interests of keeping the test run-time short
    //
    rs.r4 = Replicator(dbs.db4, createOpts(3004, [3005, 3006], 2, 4))
    rs.r5 = Replicator(dbs.db5, createOpts(3005, [3006, 3004], 2, 4))

    servers.r4 = net.createServer(function(con) {
      var server = rs.r4.createServer()
      server.pipe(con).pipe(server)
    }).listen(3004)
    
    servers.r5 = net.createServer(function(con) {
      var server = rs.r5.createServer()
      server.pipe(con).pipe(server)
    }).listen(3005)

    var failures = 0

    function done() {
      if (++failures == 2) {
        dbs.db4.get('test1key', function(err) {
          t.ok(err, 'test1key should not be in dbs.db4')
          dbs.db5.get('test1key', function(err) {
            t.ok(err, 'test1key should not be in dbs.db5')
            t.end()
          })
        })
      }
    }

    rs.r4.on('fail', done)
    rs.r5.on('fail', done)

    dbs.db4.put('test1key', 'test1value', function(err) {
      t.ok(err)
    })

    dbs.db5.put('test1key', 'test1value', function(err) {
      t.ok(err)
    })
  })



  test('joining peers in random orders at different times', function(t) {

    var base = 1000

    setTimeout(function() {

      rs.r6 = Replicator(dbs.db6, createOpts(3006, [3007, 3008], 2))

      servers.r6 = net.createServer(function(con) {
        var server = rs.r6.createServer()
        server.pipe(con).pipe(server)
      }).listen(3006)

    }, Math.ceil(Math.random()*base))

    setTimeout(function() {

      rs.r7 = Replicator(dbs.db7, createOpts(3007, [3006, 3008], 2))
      
      servers.r7 = net.createServer(function(con) {
        var server = rs.r7.createServer()
        server.pipe(con).pipe(server)
      }).listen(3007)

    }, Math.ceil(Math.random()*base))


    setTimeout(function() {

      rs.r8 = Replicator(dbs.db8, createOpts(3008, [3006, 3007], 2))
      
      servers.r8 = net.createServer(function(con) {
        var server = rs.r8.createServer()
        server.pipe(con).pipe(server)

      }).listen(3008)

      dbs.db8.put('test1key', 'test1value', function(err) {
        t.ok(!err)

        //
        // after it's been put into dbs.db8 it should be everywhere
        //
        dbs.db6.get('test1key', function(err) {
          t.ok(!err)
          dbs.db7.get('test1key', function(err) {
            t.ok(!err)
            dbs.db8.get('test1key', function(err) {
              t.ok(!err)
              t.end()
            })
          })
        })
      })

    }, Math.ceil(Math.random()*base))

  })



  test('when the databases closes, the replicator disconnects from its peers', function(t) {

    for (var r in rs)
      rs[r].close()

    for(var s in servers)
      servers[s].close()

    t.end()
  })

  t.end()
})

