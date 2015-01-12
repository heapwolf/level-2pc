# SYNOPSIS
A two-phase-commit protocol for leveldb.

# DESCRIPTION
Provides strong-consistency for local-cluster replication. 

Every node in your cluster can be writable and all reads 
from any node will be consistent.

Supports an injectable transport (so it should work in the 
browser).

# BUILD STATUS
[![Build Status](http://img.shields.io/travis/hij1nx/level-2pc.svg?style=flat)](https://travis-ci.org/hij1nx/level-2pc)

# SPECIFICATION
The algorithm for how this works is [`here`](/SPEC.md).

# CONSIDERATIONS
There are trade-offs to every type of replication. I will
try to create some specific numbers for you soon.

# USAGE

## EXAMPLE

### SERVER A
```js
var level = require('level');
var Replicator = require('level-2pc');
var net = require('net');

var db1 = level('./db', { valueEncoding: 'json' });

var opts = {

  // an array of objects that specify the host and port of each peer.
  peers: [
    { host: 'localhost', port: 3001 }, 
    { host: 'localhost', port: 3002 }
  ],

  // how many peers must connect initially or respond to quorum
  minConcensus: 2 
};

var r = Replicator(db1, opts);

net.createServer(function(con) {
  var server = r();
  server.pipe(con).pipe(server);
}).listen(3000);
```

### SERVER B
```js

var opts = { 
  peers: [
    { host: 'localhost', port: 3000 }, 
    { host: 'localhost', port: 3002 }
  ]
};

var r = Replicator(db2, opts);

net.createServer(function(con) {
  var server = r();
  server.pipe(con).pipe(server);
}).listen(3001);
```

### SERVER C
```js
var opts = { 
  peers: [
    { host: 'localhost', port: 3000 }, 
    { host: 'localhost', port: 3001 }
  ]
};

var r = Replicator(db3, opts);

net.createServer(function(con) {
  var server = r();
  server.pipe(con).pipe(server);
}).listen(3002);
```

### WRITE SOME DATA
Now go ahead and write some data to one of the
servers and watch the data magically appear in
the other servers!

```js
setTimeout(function() {
  
  db1.put('x', 100, function(err) {
    console.log(err || 'ok');
  });

  setTimeout(function() {
    db2.get('x', function() {
      console.log(arguments);
      db3.get('x', function() {
        console.log(arguments);
      });
    });
  }, 100);

}, 100);
```

# TRANSPORT
When the server wants to connect to the peers
that have been specified, it defaults to using
tcp from the `net` module. You can inject any
transportation layer you want like this...

```js
var net = require('net');
var opts = {};

opts.transport = function() {
  return net.connect.apply(null, arguments);
};

var a = replicate.createServer(db1, opts);
```

