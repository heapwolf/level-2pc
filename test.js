var level = require('level-hyper');
var net = require('net');
var rs = require('./index');
var assert = require('assert');
var rmrf = require('rimraf');
var equal = require('lodash.isEqual')

rmrf.sync('./db1');
rmrf.sync('./db2');
rmrf.sync('./db3');

//
// create a local instasnce of a database.
//
var db1 = level('./db1', { valueEncoding: 'json' });
var db2 = level('./db2', { valueEncoding: 'json' });
var db3 = level('./db3', { valueEncoding: 'json' });

//
// create server 1.
//
var opts1 = { 
  peers: [
    { host: 'localhost', port: 3001 }, 
    { host: 'localhost', port: 3002 }
  ]
};

var r1 = rs.createServer(db1, opts1);
var server1 = net.createServer(function(con) {
  r1.pipe(con).pipe(r1);
});

server1.listen(3000);

//
// create server 2.
//
var opts2 = { 
  peers: [
    { host: 'localhost', port: 3000 }, 
    { host: 'localhost', port: 3002 }
  ]
};

var r2 = rs.createServer(db2, opts2);
var server2 = net.createServer(function(con) {
  r2.pipe(con).pipe(r2);
});

server2.listen(3001);

//
// create server 3.
//
var opts2 = { 
  peers: [
    { host: 'localhost', port: 3000 }, 
    { host: 'localhost', port: 3001 }
  ]
};

var r3 = rs.createServer(db3, opts2);
var server3 = net.createServer(function(con) {
  r3.pipe(con).pipe(r3);
});

server3.listen(3002);

//
// put some data into the local server.
//

setTimeout(function() {

  var records = Array.apply(null, new Array(5000)).map(function() { 
    return { 
      key: Math.random().toString(15).slice(-100),
      value: '\0'
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

  records.forEach(function(record, index) {

    db1.put(record.key, record.value, function(err) {
      assert.ok(!err);

      if (index == records.length - 1) {
 
        setTimeout(function() {
          [db2, db3].forEach(verify);
        }, 2000); 
      }
    });
  });

  function done() {
    server1.close();
    server2.close();
    server3.close();
    process.exit(0);
  }

  function verify(db, index) {

    var results = [];
    db.createReadStream()
      .on('data', function(r) {
        results.push(r);
      })
      .on('end', function() {
        assert.ok(
          JSON.stringify(results) == JSON.stringify(records)
        );
        if (index == 1) return done(); 
      });
  }

}, 100);

