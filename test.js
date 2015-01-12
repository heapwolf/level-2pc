var level = require('level-hyper');
var net = require('net');
var Replicator = require('./index');
var rmrf = require('rimraf');
var tap = require('tap');
var test = tap.test;


function createOpts(localport, ports) {
  var peers = ports.map(function(port) {
    return { host: 'localhost', port: port };
  });
  return { peers: peers, port: localport, host: 'localhost' };
};


function createData(prefix, size) {
  //
  // generate some dummy data and sort it the way leveldb would.
  //
  var arr = Array.apply(null, new Array(size));

  return arr.map(function() { 
    return {
      key: prefix + Math.random().toString(15).slice(-256),
      value: Math.random().toString(15)
    };
  }).sort(function (a, b) {
    if (a.key > b.key) {
      return 1;
    }
    if (a.key < b.key) {
      return -1;
    }
    return 0;
  });
}


test('more than two peers', function(t) {

  var db1, db2, db3;
  var r1, r2, r3;
  var server1, server2, server3;


  //
  // create three databases and start them.
  //
  test('setup', function(t) {
    
    rmrf.sync('./db1');
    rmrf.sync('./db2');
    rmrf.sync('./db3');

    db1 = level('./db1', { valueEncoding: 'json' });
    db2 = level('./db2', { valueEncoding: 'json' });
    db3 = level('./db3', { valueEncoding: 'json' });

    //
    // create server 1.
    //
    r1 = Replicator(db1, createOpts(3001, [3002, 3003]));
    r2 = Replicator(db2, createOpts(3002, [3003, 3001]));
    r3 = Replicator(db3, createOpts(3003, [3001, 3002]));

    server1 = net.createServer(function(con) {
      var server = r1.createServer();
      server.pipe(con).pipe(server);
    }).listen(3001);
    
    server2 = net.createServer(function(con) {
      var server = r2.createServer();
      server.pipe(con).pipe(server);
    }).listen(3002);

    server3 = net.createServer(function(con) {
      var server = r3.createServer();
      server.pipe(con).pipe(server);
    }).listen(3003);

    //
    // peers should be ready for replication right away because
    // writes that are made before all connections are established
    // are queued.
    //
    t.end();
  });


  test('that a random number of records put to one peer are replicated to all other peers', function(t) {

    var size = 2;
    var index = 0;
    var records = createData('A_', size);

    records.forEach(function(record) {

      db1.put(record.key, record.value, function(err) {
        t.ok(!err);

        //
        // after the last record is put, all records should be in the database.
        //

        if (++index == records.length) {
          [db1, db2, db3].forEach(verifyRecords);
        }
      });
    });

    function verifyRecords(db, index) {

      var results = [];
      var count = 0;

      db.createReadStream({ gte: 'A_', lte: 'A_~' })
        .on('data', function(r) {
          ++count;
          results.push(r);
        })
        .on('end', function() {
          t.equal(count, records.length)
          t.equal(
            JSON.stringify(results), JSON.stringify(records)
          );
          if (index == 1) return t.end(); 
        });
    }
  });


  test('that records batched to one peer are replicated to all other peers', function(t) {

    function chunk(arr, n) {
      return !arr.length ? [] : [arr.slice(0, n)].concat(chunk(arr.slice(n), n));
    };

    var size = 10;
    var records = createData('B_', size);
    var groups = chunk(records, 2);

    groups.forEach(function(group, index) {
      var newgroup = [];
      group.forEach(function(op) {
        newgroup.push({ type: 'put', key: op.key, value: op.value });
      });

      db1.batch(newgroup, function(err) {
        t.ok(!err);
        if (index == groups.length-1) {
          [db2, db3].forEach(verifyRecords);
        }
      });
    });

    function verifyRecords(db, index) {
      var results = [];
      db.createReadStream({ gte: 'B_', lte: 'B_~' })
        .on('data', function(r) {
          results.push(r);
        })
        .on('end', function() {
          t.equal(
            JSON.stringify(results), JSON.stringify(records)
          );
          if (index == 1) return t.end(); 
        });
    }
  });


  test('that a single record is added to all peers before returning the callback', function(t) {
    
    db1.put('test1key', 'test1value', function(err) {
      t.ok(!err, 'key added to the coordinator and all other peers');
      db2.get('test1key', function(err) {
        t.ok(!err, 'key was found in db2');
        db3.get('test1key', function(err) {
          t.ok(!err, 'key was found in db3');
          t.end();
        });
      });
    });
  });



  test('that a record can be deleted from one database and it is removed from all others', function(t) {
    
    db1.del('test1key', function(err) {
      t.ok(!err, 'key added to the coordinator and all other peers');
      db2.get('test1key', function(err) {
        t.ok(err, 'key was not found in db2');
        db3.get('test1key', function(err) {
          t.ok(err, 'key was not found in db3');
          t.end();
        });
      });
    });
  });


  test('a peer that is not reachable, replication still happens between available peers', function(t) {
    t.end();
  });


  test('that replication fails if the minimum concensus can not be reached', function(t) {
    t.end();
  });


  test('when the databases closes, the replicator disconnects from its peers', function(t) {
    server1.close();
    server2.close();
    server3.close();
    db1.close();
    db2.close();
    db3.close();
    t.end();
  });

  t.end();
});

