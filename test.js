var level = require('level-hyper');
var net = require('net');
var rs = require('./index');
var assert = require('assert');

//
// create a local instasnce of a database.
//
var db1 = level('./db1', { valueEncoding: 'json' });
var db2 = level('./db2', { valueEncoding: 'json' });
var db3 = level('./db3', { valueEncoding: 'json' });

//
// create server 1.
//
var r1 = rs.createServer(db1);
r1.peers = [
  { host: 'localhost', port: 3001 }, 
  { host: 'localhost', port: 3002 }
];

var server1 = net.createServer(function(con) {
  r1.pipe(con).pipe(r1);
});

server1.listen(3000);

//
// create server 2.
//
var r2 = rs.createServer(db2);
r2.peers = [
  { host: 'localhost', port: 3000 }, 
  { host: 'localhost', port: 3002 }
];

var server2 = net.createServer(function(con) {
  r2.pipe(con).pipe(r2);
});

server2.listen(3001);

//
// create server 3.
//
var r3 = rs.createServer(db3);

r3.peers = [
  { host: 'localhost', port: 3000 }, 
  { host: 'localhost', port: 3001 }
];

var server3 = net.createServer(function(con) {
  r3.pipe(con).pipe(r3);
});

server3.listen(3002);

//
// put some data into the local server.
//
setTimeout(function() {
  
  db1.put('x', 100, function(err) {
    assert.ok(!err);
  });

  setTimeout(function() {
    db2.get('x', function(err, val) {
      assert.equal(val, 100);
      db3.get('x', function() {
        assert.equal(val, 100);
        server1.close();
        server2.close();
        server3.close();
        process.exit(0);
      });
    });
  }, 100);

}, 100);

