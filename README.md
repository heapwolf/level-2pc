# SYNOPSIS
A two-phase-commit protocol for leveldb.

# DESCRIPTION
Provides strong-consistency for local-cluster replication.

Every node in your cluster can be writable and all reads
from any node will be consistent.

Uses [`reconnect-core`](https://github.com/juliangruber/reconnect-core) to support an injectable transport for e.g. browser compatibility.

# BUILD STATUS
[![Build Status](http://img.shields.io/travis/hij1nx/level-2pc.svg?style=flat)](https://travis-ci.org/hij1nx/level-2pc)

# SPECIFICATION
The algorithm for how this works is [`here`](/SPEC.md).

# USAGE

## EXAMPLE

### SERVER A
```js
var level = require('level');
var Replicator = require('level-2pc');
var net = require('net');

var db1 = level('./db', { valueEncoding: 'json' });

var opts = {
  peers: [
    { host: 'localhost', port: 3001 },
    { host: 'localhost', port: 3002 }
  ]
};

var r = Replicator(db1, opts);

net.createServer(function(con) {
  var server = r.createServer();
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
  var server = r.createServer();
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
  var server = r.createServer();
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
transportation layer you like by setting the
`transport` property in the options object:

```js
var net = require('net');

var opts = {
  transport: function() {
    return net.connect.apply(null, arguments);
  },
  peers: [ /* .. */ ]
};

var r = Replicator(db, opts);
```

# API

### Replicator(db, opts)

Returns a `Replicator` object, which is an `EventEmitter`.

* `db` leveldb database object
* `opts` options object with the following properties:

  * `host` host that other peers should connect to
  * `port` port that other peers should connect to
  * `peers` an array of objects that specify the host and port of each peer
  * `minConcensus` how many peers must connect initially or respond to quorum

### Replicator#createServer()

Returns a duplex [`rpc-stream`](https://github.com/dominictarr/rpc-stream) that can be served over e.g. `http` or `tcp` or any other transport supporting node streams.

### Replicator#close()

Closes connections to all peers.

### Event: 'ready'

Emitted when the replicator is ready to replicate with other peers. Happens when the replicator has enough connections for the quorum, i.e. when the number of peers is above `minConcensus`.

### Event: 'notready'

Emitted when the replicator is not ready to replicate with other peers. Happens when the replicator doesn't have enough connections for the quorum, i.e. when the number of peers goes below `minConcensus`.

### Event: 'connect'

Emitted when the replicator has connected to a peer.

* `host` host of the connected peer
* `port` port of the connected peer

### Event: 'disconnect'

Emitted when the replicator has diconnected from a peer.

* `host` host of the disconnected peer
* `port` port of the disconnected peer

### Event: 'reconnect'

Emitted when the replicator tries to reconnect to a peer.

* `host` retrying connection to this host
* `port` retrying connection to this port

### Event: 'fail'

Emitted when the replicator has tried to reconnect but failed too many times. There might be a problem with the connection, or the peer is simply offline.

* `host` host of the failing peer
* `port` port of the failing peer