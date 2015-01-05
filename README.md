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

# REPLICATION METHODS

## SYNCHRONOUS REPLICATION (SYNC)

Write is not considered complete until all peers have acknowledged the write. Strong Consistency. Unfortunately in this mode, if a single peer goes down, then replication will not take place across the remaining peers. 

## SEMI-SYNCHRONOUS REPLICATION (SEMISYNC) (DEFAULT)

The write is considered complete as soon as all connected and alive peers confirm the write. In this mode, if a peer goes down, it is removed from the peers required to acknowledge the write. This still provides strong consistency for all the connected peers, but better fault tolerance and flexibility for adding new peers and for peers going down for maintainance or network distruptions.

# USAGE

## EXAMPLE

### SERVER A
```js
var level = require('level');
var replicate = require('level-2pc');
var net = require('net');

var db1 = level('./db', { valueEncoding: 'json' });

var opts = { 
  peers: [
    { host: 'localhost', port: 3001 }, 
    { host: 'localhost', port: 3002 }
  ]
};

var a = replicate.createServer(db1, opts);

net.createServer(function(con) {
  a.pipe(con).pipe(a);
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

var b = rs.createServer(db2, opts);

net.createServer(function(con) {
  b.pipe(con).pipe(b);
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

var c = rs.createServer(db3);

net.createServer(function(con) {
  c.pipe(con).pipe(c);
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

