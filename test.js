var level = require('level-hyper')
var net = require('net')
var after = require('after')
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
    minConsensus: min,
    failAfter: failAfter || 16,
    host: 'localhost'
  }
}


function createData(prefix, size, type) {

  var map = function() { 
    if (type == 'json') {
      return {
        key: prefix + Math.random().toString(15).slice(-256),
        value: { string: Math.random().toString(15) }
      }
    } else {
      return {
        key: prefix + Math.random().toString(15).slice(-256),
        value: Math.random().toString(15)
      }
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

  var i = 16
  var dbs = {}
  var rs = {}
  var servers = {}

  while(i > 0) {
    rmrf.sync('./test/db' + i)
    dbs['db' + i] = level('./test/db' + i, { keyEncoding: 'utf8', valueEncoding: 'utf8' })
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
    var records = createData('A_', size)

    var done = after(records.length, function (err) {
      t.ok(!err)
      var databases = [dbs.db1, dbs.db2, dbs.db3]
      var done2 = after(databases.length, t.end.bind(t))
      databases.forEach(function (db) {
        verifyRecords(db, done2)
      })
    })

    records.forEach(function(record) {
      dbs.db1.put(record.key, record.value, { valueEncoding: 'utf8' }, done)
    })

    function verifyRecords(db, cb) {
      var results = []
      var count = 0

      db.createReadStream({ gte: 'A_', lte: 'A_~' })
        .on('data', function(r) {
          ++count
          results.push(r)
        })
        .on('end', function() {
          t.equal(count, records.length, 'equal #1')
          t.equal(
            JSON.stringify(results), JSON.stringify(records), 'equal #2'
          )
          cb()
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

    var done = after(groups.length, function (err) {
      t.ok(!err)
      var databases = [dbs.db2, dbs.db3]
      var done2 = after(databases.length, t.end.bind(t))
      databases.forEach(function (db) {
        verifyRecords(db, done2)
      })
    })

    groups.forEach(function(group) {
      var newgroup = []
      group.forEach(function(op) {
        newgroup.push({ type: 'put', key: op.key, value: op.value })
      })
      dbs.db1.batch(newgroup, done)
    })

    function verifyRecords(db, cb) {
      var results = []
      db.createReadStream({ gte: 'B_', lte: 'B_~' })
        .on('data', function(r) {
          results.push(r)
        })
        .on('end', function() {
          t.equal(
            JSON.stringify(results), JSON.stringify(records), 'equal #3'
          )
          cb()
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

  test('peer discovery -- peer joins later on', function(t) {

    rs.r9 = Replicator(dbs.db9, createOpts(3009, [3010], 1))

    servers.r9 = net.createServer(function(con) {
      var server = rs.r9.createServer()
      server.pipe(con).pipe(server)
    }).listen(3009)

    rs.r10 = Replicator(dbs.db10, createOpts(3010, [3009], 1))

    servers.r10 = net.createServer(function(con) {
      var server = rs.r10.createServer()
      server.pipe(con).pipe(server)
    }).listen(3010)


    dbs.db9.put('test1key', 'test1value', function(err) {
      t.ok(!err)
      dbs.db10.get('test1key', function(err) {
        t.ok(!err);
      })
    })

    setTimeout(function() {

      rs.r11 = Replicator(dbs.db11, createOpts(3011, [3009, 3010], 1))

      servers.r11 = net.createServer(function(con) {
        var server = rs.r11.createServer()
        server.pipe(con).pipe(server)

      }).listen(3011)

      dbs.db11.put('test2key', 'test2value', function(err) {
        t.ok(!err)

        //
        // after it's been put into dbs.db8 it should be everywhere
        //
        dbs.db9.get('test2key', function(err) {
          t.ok(!err)
          dbs.db10.get('test2key', function(err) {
            t.ok(!err)
            t.end();
          })
        })
      })

    }, Math.ceil(Math.random()*1000))

  })

  test('random set of json encoded records pass', function(t) {

    var size = 2
    var records = createData('C_', size, 'json')

    var done = after(records.length, function (err) {
      t.ok(!err)
      var databases = [dbs.db1, dbs.db2, dbs.db3]
      var done2 = after(databases.length, t.end.bind(t))
      databases.forEach(function (db) {
        verifyRecords(db, done2)
      })
    })

    records.forEach(function(record) {
      dbs.db1.put(record.key, record.value, { keyEncoding: 'utf8', valueEncoding: 'json' }, done)
    })

    function verifyRecords(db, cb) {
      var results = []
      var count = 0

      db.createReadStream({ gte: 'C_', lte: 'C_~', valueEncoding: 'json' })
        .on('data', function(r) {
          ++count
          results.push(r)
        })
        .on('end', function() {
          t.equal(count, records.length, 'equal #4')
          t.equal(
            JSON.stringify(results), JSON.stringify(records), 'equal #5'
          )
          cb()
        })
    }
  })

  test('closed peers should not try to reconnect if missing failAfter', function(t) {
    var opts12 = createOpts(3012, [3013], 1)
    delete opts12.failAfter
    var r12 = Replicator(dbs.db12, opts12)

    var opts13 = createOpts(3013, [3012], 1)
    delete opts13.failAfter
    var r13 = Replicator(dbs.db13, opts13)

    var s12 = net.createServer(function(con) {
      var server = r12.createServer()
      server.pipe(con).pipe(server)
    }).listen(3012)

    var s13 = net.createServer(function(con) {
      var server = r13.createServer()
      server.pipe(con).pipe(server)
    }).listen(3013)

    var ready = after(2, thirdPeer)

    function thirdPeer(err) {
      t.ok(!err)

      var opts14 = createOpts(3014, [3012, 3013], 2)
      delete opts14.failAfter
      var r14 = Replicator(dbs.db14, opts14)

      var s14 = net.createServer(function(con) {
        var server = r14.createServer()
        server.pipe(con).pipe(server)
      }).listen(3014)

      var reconnectAttempts = 0
      function attemptedReconnect() { ++reconnectAttempts }

      r12.on('reconnect', attemptedReconnect)
      r13.on('reconnect', attemptedReconnect)
      r14.on('reconnect', attemptedReconnect)

      r14.once('ready', function () {

        var notready = after(3, function (err) {
          t.ok(!err, 'no error')
          setTimeout(function () {
            t.equal(reconnectAttempts, 0, 'no attempts to reconnect')
            t.end()
          }, 500)
        })

        r12.once('notready', notready)
        r13.once('notready', notready)
        r14.once('notready', notready)

        s12.close()
        s13.close()
        s14.close()

        r12.close()
        r13.close()
        r14.close()

      })
    }

    r12.once('ready', ready)
    r13.once('ready', ready)
  })

  test('peer with minConsensus 0 should always be in ready state', function(t) {
    var peer1, peer2
    var servers = []

    var ready = after(2, function() {
      peer1.close()
      peer2.close()
      servers.forEach(function(server) { server.close() })
    })

    peer1 = Replicator(dbs.db15, createOpts(3015, [3016], 0))

    servers.push(net.createServer(function(con) {
      var server = peer1.createServer()
      server.pipe(con).pipe(server)
    }).listen(3015))

    peer1.on('ready', ready)
    peer1.on('notready', function () {
      t.fail('should never happen')
    })

    peer2 = Replicator(dbs.db16, createOpts(3016, [3015], 1))

    servers.push(net.createServer(function(con) {
      var server = peer2.createServer()
      server.pipe(con).pipe(server)
    }).listen(3016))

    peer2.on('ready', ready)
    peer2.on('notready', function() {
      t.ok(true, 'only one notready called')
      t.end()
    })

  })

  test('when the databases closes, the replicator disconnects from its peers', function(t) {

    var done = after(Object.keys(servers).length, function (err) {
      t.ok(!err, 'no error')
      t.end()
    })

    for (var r in rs)
      rs[r].close()

    for(var s in servers)
      servers[s].close(done)

  })

  t.end()
})

