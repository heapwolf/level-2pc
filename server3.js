var level = require('level-hyper');
var net = require('net');
var rs = require('./index');
var rmrf = require('rimraf');


rmrf.sync('./db8');
db8 = level('./db8', { valueEncoding: 'json' });

r1 = rs.createServer(db8, {host: 'localhost', port: 3002, peers:[{host:'localhost',port:3001}]});
server1 = net.createServer(function(con) {
  r1.pipe(con).pipe(r1);
});

server1.listen(3002);

setTimeout(function() {
  db8.put(Math.random().toString(15).slice(-256), Math.random().toString(15).slice(-256), function(err) {
  });
}, 5000)
