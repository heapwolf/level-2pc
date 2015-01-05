var rpc = require('rpc-stream');
var createClient = require('./client');
var net = require('net');
var ttl = require('level-ttl');
var debug = require('debug')('level2pc');

var prefix = '\xffxxl\xff';
var ttlk = '\xffttl\xff';
var HR1 = { ttl: 1000 * 60 * 60 };
var local = {}

function Server(localdb, config) {

  config = config || {};
  ttl(localdb);

  local = {host: config.host, port: config.port};

  var db = {
    batch: localdb.batch.bind(localdb),
    put: localdb.put.bind(localdb),
    del: localdb.del.bind(localdb)
  };

  function prefixOps(arr, prefix_peers) {
    var ops = [];
    arr.map(function(op) {
      ops.push({ key: prefix + peerId(local) + '\xff' + op.key, value: op.value, type: op.type });
      if (prefix_peers) {
        config.peers.map(function(peer) {
          ops.push({ key: prefix + peerId(peer) + '\xff' + op.key, value: op.value, type: op.type })
        })
      }
    });
    return ops;
  }
  
  function peerId(peer) {
    return peer.port + peer.host;
  }

  localdb.del = function(key, cb) {

    if (key.indexOf(ttlk) == 0)
      return del.apply(localdb, arguments);

    var ops = [{ type: 'del', key: prefix + peerId(local) + '\xff' + key }];
    config.peers.forEach(function(peer) {
      ops.push({ type: 'del', key: prefix + peerId(peer) + '\xff' + key })
    });

    db.batch(ops, function(err) {
      if (err) return cb(err);
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

    var ops = [{ type: 'put', key: prefix + peerId(local) + '\xff' + key, value: val, opts: opts }];  
    config.peers.forEach(function(peer) {
      ops.push({ type: 'put', key: prefix + peerId(peer) + '\xff' + key, value: val, opts: opts });
    });

    db.batch(ops, function(err) {
      if (err) return cb(err);
      var op = { type: 'put', key: key, value: val, opts: opts };
      replicate(op, cb);
    })
    
    /*
    db.put(prefix + key, val, opts, function(err) {
      var op = { type: 'put', key: key, value: val, opts: opts };
      replicate(op, cb);
    });
    */
  };

  localdb.batch = function(arr, opts, cb) {
    if (arr[0] && arr[0].key.indexOf(ttlk) == 0)
      return db.batch.apply(localdb, arguments);

    if ('function' == typeof opts) {
      cb = opts;
      opts = {};
    }

    db.batch(prefixOps(arr, true), opts, function(err) {
      var op = { type: 'batch', value: arr };
      replicate(op, cb);
    });
  };

  config.peers = config.peers || [];
  var methods = {};

  methods.quorum = function (op, peer, cb) {
    debug('QUORUM PHASE @', config.host, config.port, peer)
    
    if (op.type == 'batch') {
      db.batch(prefixOps(op.value, false), op.opts, cb);
    }
    else if (op.type == 'put') {
      db.put(prefix + peerId(peer) + '\xff' + op.key, op.value, op.opts, cb);
    }
    else if (op.type == 'del') {
      db.del(prefix + peerId(peer) + '\xff' + op.key, cb);
    }
  };
  
  methods.commit = function (op, peer, cb) {
    debug('COMMIT PHASE @', peer.host, peer.port)

    if (op.type == 'batch') {
      op.value.forEach(function(o) {
        op.value.push({ type: 'del', key: prefix + peerId(local) + '\xff' + o.key })
      });
    }
    else {
      op.value = [
        { type: 'del', key: prefix + peerId(local) + '\xff' + op.key },
        { type: op.type, key: op.key, value: op.value }
      ];
    }

    op.value.push({ type: 'del', key: prefix + peerId(peer) + '\xff' + op.key })

    op.opts = op.opts || { ttl: config.ttl };

    db.batch(op.value, op.opts, cb);
  };


  var server = rpc(methods);
  var connections = {};
  var clients = [];
  var connected_peers = [];
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
      debug('PEER CONNECTED @', peer);
      var r = rpc();
      remote = r.wrap(methods);
      connections[peer.port + peer.host] = r.wrap(methods);
      r.pipe(s).pipe(r);

      syncPeer(peer);

      connected_peers.push(peer);
      
    });

    client.on('disconnect', function() {
      if (connected_peers.indexOf(peer) == 1)
        debug('PEER DISCONNECTED @', peer);

      if (connected_peers.indexOf(peer) != -1)
        connected_peers.splice(connected_peers.indexOf(peer), 1)
    });

    clients.push(client);

    if (config.peers.indexOf(peer) == -1) {
      config.peers.push(peer);
    }
  };

  config.peers.forEach(server.addPeer);
  loaded = true;

  function replicatePeers(op, cb) {
    debug('CONNECTED PEERS @', connected_peers);

    var phase = 'quorum';
    var index = 0;

    if (connected_peers.length == 0)
      return cb(null, []);

    !function next() {
      connected_peers.map(function(peer) {
        debug('COORDINATING PEER @', config.host, config.port, peer)

        var remote = connections[peer.port + peer.host];
      
        function write() {
          debug('WRITE PHASE @', phase, peer)
          remote[phase](op, peer, function(err) {
            if (err) {
              return cb(err);
            }

            if (++index == connected_peers.length) {
              if (phase != 'quorum') {
                return cb(null, connected_peers);
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

  function confirmPeers(op, peers, cb) {

    if (peers.length == 0)
      return cb();

    var ops = [];

    peers.map(function(peer) {
      if (op.type == 'batch') {
        op.value.forEach(function(o) {
          ops.push({ type: 'del', key: prefix + peerId(peer) + '\xff' + o.key })
        });
      }
      else {
        ops.push({ type: 'del', key: prefix + peerId(peer) + '\xff' + op.key });
      }
    });

    db.batch(ops, cb);
  }
  
  function syncPeer(peer) {
    debug('SYNC PEER EVENT @', local)

    var ops = [];
    localdb.createReadStream({
      gte: prefix + peerId(peer) + '\xff',
      lte: prefix + peerId(peer) + '\xff~'
    })
    .on('data', function(data) {
      debug('SYNC PEER DATA @', data);
      ops.push({type: 'put', key: data.key.replace(prefix + peerId(peer) + '\xff', ''), value: data.value});
    })
    .on('end', function() {
      if (ops.length == 0) return;

      var remote = connections[peer.port + peer.host];
      
      var op = { type: 'batch', value: ops };
      debug('SYNC PEER OP @', op );
      remote['commit'](op, peer, function(err) {
        if (err) return debug('SYNC PEER ERROR @', err);
        
        confirmPeers(op, [peer], function(err) {
          if (err) return debug('SYNC CONFIRM PEER ERROR @', err);
        })
      });
    });
  }

  function replicate(op, cb) {
    debug('REPLICATION EVENT @', config.host, config.port)
    replicatePeers(op, function(err, peers) {
      if (err) return cb(err);
      methods.commit(op, local, function(err) {
        if (err) return cb(err);
        confirmPeers(op, peers, cb);
      });
    });
  }

  var close = localdb.close;

  localdb.close = function() {
    clients.map(function(client) {
      client.disconnect();
    });
    close.apply(localdb, arguments);
  };

  return server;
}

exports.Server = Server;
exports.createServer = Server;

