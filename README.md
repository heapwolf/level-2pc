# SYNOPSIS
A two-phase-commit protocol for leveldb.

# DESCRIPTION
Provides strong-consistency for levevldb replication.

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
var replicate = require('level-2pc');
var net = require('net');

var db1 = level('./db', { valueEncoding: 'json' });

var a = replicate.createServer(db1);

a.peers = [
  { host: 'localhost', port: 3001 }, 
  { host: 'localhost', port: 3002 }
];

net.createServer(function(con) {
  a.pipe(con).pipe(a);
}).listen(3000);
```

### SERVER B
```js
var b = rs.createServer(db2);

b.peers = [
  { host: 'localhost', port: 3000 }, 
  { host: 'localhost', port: 3002 }
];

net.createServer(function(con) {
  b.pipe(con).pipe(b);
}).listen(3001);
```

### SERVER C
```js
var c = rs.createServer(db3);

c.peers = [
  { host: 'localhost', port: 3000 }, 
  { host: 'localhost', port: 3001 }
];

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

