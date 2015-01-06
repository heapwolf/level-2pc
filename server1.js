var level = require('level-hyper');
var net = require('net');
var rs = require('./index');
var rmrf = require('rimraf');


rmrf.sync('./db6');
db6 = level('./db6', { valueEncoding: 'json' });
db6.put('testkey1', 'value1');

//r1 = rs.createServer(db6, {host: 'localhost', port: 3000, peers:[{host:'localhost',port:3001}]});
r1 = rs.createServer(db6, {host: 'localhost', port: 3000});
server1 = net.createServer(function(con) {
  r1.pipe(con).pipe(r1);
});

server1.listen(3000);


setTimeout(function() {
  for (var i=0; i<5; i++) {
    db6.put('A__' + Math.random().toString(15).slice(-256), Math.random().toString(15).slice(-256), function(err) {
    });
  } 
}, 5000);
