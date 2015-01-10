var Replicator = require('./index');
var net = require('net');
var rpc = require('rpc-stream');
var level = require('level-hyper');

var db1 = level('./db1', { valueEncoding: 'json' });
var db2 = level('./db2', { valueEncoding: 'json' });
var db3 = level('./db3', { valueEncoding: 'json' });

var peers1 = [{ port: 9002, host: 'localhost' }, { port: 9003, host: 'localhost' }];
var peers2 = [{ port: 9001, host: 'localhost' }, { port: 9003, host: 'localhost' }];
var peers3 = [{ port: 9001, host: 'localhost' }, { port: 9002, host: 'localhost' }];


var r1 = Replicator(db1, { peers: peers1, port: 9001, host: 'localhost' });
var r2 = Replicator(db2, { peers: peers2, port: 9002, host: 'localhost' });
var r3 = Replicator(db3, { peers: peers3, port: 9003, host: 'localhost' });



var server1 = net.createServer(function(con) {
  var server = r1();
  server.pipe(con).pipe(server);

}).listen(9001);



var server2 = net.createServer(function(con) {
  var server = r2();
  server.pipe(con).pipe(server);

}).listen(9002);


var server3 = net.createServer(function(con) {
  var server = r3();
  server.pipe(con).pipe(server);

}).listen(9003);



//setTimeout(function() {
//  db2.close();
//  server2.close();
//  console.log(server2)
//}, 100);

//setTimeout(function() {

  db1.put('foo', 100, function(err) {
    console.log('put foo', err);
  });

//}, 1000);

