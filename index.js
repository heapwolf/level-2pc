var rpc = require('rpc-stream');
var createClient = require('./client');
var net = require('net');
var ttl = require('level-ttl');
var debug = require('debug')('level2pc');

var prefix = '\xffxxl\xff';
var ttlk = '\xffttl\xff';
var HR1 = { ttl: 1000 * 60 * 60 };

function Server(localdb, config) {
 
  config = config || {};
  ttl(localdb);

  var db = {
    batch: localdb.batch.bind(localdb),
    put: localdb.put.bind(localdb),
    del: localdb.del.bind(localdb)
  };

  function prefixOps(arr) {
    var ops = [];
    arr.map(function(op) {
      ops.push({ key: prefix + op.key, value: op.value, type: op.type });
    });
    return ops;
  }

  localdb.del = function(key, cb) {

    if (key.indexOf(ttlk) == 0)
      return del.apply(localdb, arguments);

    db.del(prefix + key, function(err) {
      var op = { type: 'del', key: key };
      replicate(op, cb);
    });
  };

  localdb.put = function(key, val, opts, cb) {

    if ('function' == typeof val) {
      throw new TypeError('Put expects a value not a function');
    }

    if ('function' == typeof opts) {
      cb = opts;
      opts = {};
    }

    db.put(prefix + key, val, opts, function(err) {
      var op = { type: 'put', key: key, value: val, opts: opts };
      replicate(op, cb);
    });
  };

  localdb.batch = function(arr, opts, cb) {
    if (arr[0] && arr[0].key.indexOf(ttlk) == 0)
      return db.batch.apply(localdb, arguments);

    if ('function' == typeof opts) {
      cb = opts;
      opts = {};
    }

    db.batch(prefixOps(arr), opts, function(err) {
      var op = { type: 'batch', value: arr };
      replicate(op, cb);
    });
  };

  config.peers = config.peers || [];
  var methods = {};

  methods.quorum = function (op, cb) {
    debug('QUORUM PHASE @', config.host, config.port)
    
    if (op.type == 'batch') {
      db.batch(prefixOps(op.value), op.opts, cb);
    }
    else if (op.type == 'put') {
      db.put(prefix + op.key, op.value, op.opts, cb);
    }
    else if (op.type == 'del') {
      db.del(prefix + op.key, cb);
    }
  };
  
  methods.commit = function (op, cb) {
    debug('COMMIT PHASE @', config.host, config.port)

    if (op.type == 'batch') {
      op.value.forEach(function(o) {
        op.value.push({ type: 'del', key: prefix + o.key })
      });
    }
    else {
      op.value = [
        { type: 'del', key: prefix + op.key },
        { type: op.type, key: op.key, value: op.value }
      ];
    }

    op.opts = op.opts || { ttl: config.ttl };

    db.batch(op.value, op.opts, cb);
  }; 

  var server = rpc(methods);
  var connections = {};
  var clients = [];
  var loaded;

  function connectionError(host, port) {
    return new Error('Connection failed to ' + host + ':' + port);
  }

  server.addPeer = function(peer) {

    if (loaded && config.peers.some(function(p) {
      var host = p.host == peer.host;
      var port = p.port == peer.port;
      return host && port;
    })) return;

    client = createClient(config);
    client.connect(peer.port, peer.host);
    client.on('error', function(err) {
      server.emit('error', err);
    });

    client.on('fail', function() {
      server.emit('error', connectionError(peer.host, peer.port));
    });

    client.on('connect', function(s) {
      var r = rpc();
      remote = r.wrap(methods);
      connections[peer.port + peer.host] = r.wrap(methods);
      r.pipe(s).pipe(r);
    });

    if (config.peers.indexOf(peer) == -1) {
      config.peers.push(peer);
    }

    clients.push(client);
  };

  config.peers.forEach(server.addPeer);
  loaded = true;

  function getQuorum(op, cb) {

    var phase = 'quorum';
    var index = 0;

    !function next() {
      config.peers.map(function(peer) {
        debug('COORDINATING PEER @', config.host, config.port, peer)

        var remote = connections[peer.port + peer.host];
      
        function write() {
          remote[phase](op, function(err) {
            if (err) {
              return cb(err);
            }

            if (++index == config.peers.length) {
              if (phase != 'quorum') {
                return cb();
              }

              phase = 'commit';
              index = 0;
              next();
            }
          });
        }

        if (remote) {
          return write();
        }

        var retrycount = 0;

        function tryRemote() {
          remote = connections[peer.port + peer.host];

          if (remote) {
            clearInterval(retry);
            return write();
          }

          if (++retrycount == config.failAfter) {
            clearInterval(retry);
            cb(connectionError(peer.host, peer.port));
            return cb = function() {};
          }
        }

        var retry = setInterval(tryRemote, 10);
        tryRemote();

      });
    }();
  }

  function replicate(op, cb) {
    debug('REPLICATION EVENT @', config.host, config.port)
    getQuorum(op, function(err) {
      if (err) return cb(err);
      methods.commit(op, cb);
    });
  }

  var close = localdb.close;

  localdb.close = function() {
    clients.map(function(client) {
      client.disconnect();
    });
    close();
  };

  return server;
}

exports.Server = Server;
exports.createServer = Server;

