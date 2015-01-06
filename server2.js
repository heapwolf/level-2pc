var level = require('level-hyper');
var net = require('net');
var rs = require('./index');
var rmrf = require('rimraf');


rmrf.sync('./db7');
db7 = level('./db7', { valueEncoding: 'json' });

r1 = rs.createServer(db7, {host: 'localhost', port: 3001, peers:[{host:'localhost',port:3000}]});
server1 = net.createServer(function(con) {
  r1.pipe(con).pipe(r1);
});

server1.listen(3001);
