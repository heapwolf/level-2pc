var rpc = require('rpc-stream');
var createClient = require('./client');
var net = require('net');
var ttl = require('level-ttl');
var xtend = require('xtend');

var prefix = '\xffxxl\xff';
var ttlk = '\xffttl\xff';
var HR1 = { ttl: 1000 * 60 * 60 };

function Server(localdb, config) {

  ttl(localdb);

  var config = xtend({mode: 'semisync'}, config);
  config.name = (config.port+config.host).toUpperCase();
  config.peers = config.peers || [];

  var debug = require('debug')('level2pc:' + config.name);

  var local_count = 0;
  var local_peer = {host: config.host, port: config.port};

  var connections = {};
  var clients = [];
  var connected_peers = [];
  var loaded;
  var ready;
  var reconcile;
  var ready_peers = [];

  var db = {
    batch: localdb.batch.bind(localdb),
    put: localdb.put.bind(localdb),
    del: localdb.del.bind(localdb)
  };

  function connectionError(host, port) {
    return new Error('Connection failed to ' + host + ':' + port);
  }

  function isLocalPeer(peer) {
    var result = (local_peer.host == peer.host && local_peer.port == peer.port);
    debug('isLocalPeer', result, peer);
    return result;
  }

  function containsPeer(peers, peer) {
    return peers.some(function(p) {
      var host = p.host == peer.host;
      var port = p.port == peer.port;
      return host && port;
    })
  }

  function prefixOps(arr, prefix_peers) {
    var ops = [];
    arr.map(function(op) {
      ops.push({ key: prefixPeer(local_peer) + op.key, value: op.value, type: op.type });
      if (prefix_peers) {
        config.peers.map(function(peer) {
          ops.push({ key: prefixPeer(peer) + op.key, value: op.value, type: op.type })
        })
      }
    });
    return ops;
  }
  
  function prefixPeer(peer) {
    return prefix + peer.port + peer.host + '\xff';
  }

  localdb.del = function(key, cb) {

    if (key.indexOf(ttlk) == 0)
      return del.apply(localdb, arguments);

    var ops = [{ type: 'del', key: prefixPeer(local_peer) + key }];
    config.peers.forEach(function(peer) {
      ops.push({ type: 'del', key: prefixPeer(peer) + key })
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

    var ops = [{ type: 'put', key: prefixPeer(local_peer) + key, value: val, opts: opts }];  
    config.peers.forEach(function(peer) {
      ops.push({ type: 'put', key: prefixPeer(peer) + key, value: val, opts: opts });
    });

    db.batch(ops, function(err) {
      if (err) return cb(err);
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

    db.batch(prefixOps(arr, true), opts, function(err) {
      var op = { type: 'batch', value: arr };
      replicate(op, cb);
    });
  };

  var close = localdb.close;

  localdb.close = function() {
    clients.map(function(client) {
      client.disconnect();
    });
    close.apply(localdb, arguments);
  };



  var methods = {};

  methods.quorum = function (op, peer, cb) {
    debug('QUORUM PHASE @', peer.host, peer.port)
    
    if (op.type == 'batch') {
      db.batch(prefixOps(op.value, false), op.opts, cb);
    }
    else if (op.type == 'put') {
      db.put(prefixPeer(peer) + op.key, op.value, op.opts, cb);
    }
    else if (op.type == 'del') {
      db.del(prefixPeer(peer) + op.key, cb);
    }
  };

  methods.commit = function (op, peer, cb) {
    debug('COMMIT PHASE @', peer.host, peer.port)

    if (op.type == 'batch') {
      op.value.forEach(function(o) {
        op.value.push({ type: 'del', key: prefixPeer(local_peer) + o.key })
      });
    }
    else {
      op.value = [
        { type: 'del', key: prefixPeer(local_peer) + op.key },
        { type: op.type, key: op.key, value: op.value }
      ];
    }

    op.value.push({ type: 'del', key: prefixPeer(peer) + op.key })

    op.opts = op.opts || { ttl: config.ttl };

    db.batch(op.value, op.opts, cb);
  };
  
  methods.ready = function(peer) {
    debug('PEER READY @', peer);
    if (!containsPeer(ready_peers, peer)) {
      ready_peers.push(peer); 
    }
  };
  
  methods.sync = function(sync_peer, cb) {
    debug('PEER SYNC @', sync_peer.host, sync_peer.port);
    debug('PEER PREFIX', prefix);

    localdb.createReadStream()
      .on('data', function(data) {
        if (data.key.indexOf(prefix) > -1) return;

        db.put(prefixPeer(sync_peer) + data.key, data.value);
      })
      .on('end', cb);
  };

  methods.addPeers = function(peers) {
    peers.forEach(methods.addPeer);
  };

  methods.addPeer = function(peer) {
    if (isLocalPeer(peer)) {
      debug('addPeer.isLocalPeer', peer)
      return cb;
    }
    if (loaded && containsPeer(config.peers, peer)) {
      debug('addPeer.alreadyConfigured', peer);
      return cb;
    }

    debug('ADD PEER @', peer);

    config.peers.push(peer);

    var client = createClient(config);
    client.connect(peer.port, peer.host);
    client.on('error', function(err) {
      server.emit('error', err);
    });

    client.on('fail', function() {
      server.emit('error', connectionError(peer.host, peer.port));
    });

    client.on('connect', function(s) {
      debug('PEER CONNECTED @', peer);

      connected_peers.push(peer);

      var r = rpc();
      r.pipe(s).pipe(r);

      var r_remote = r.wrap(methods);
      connections[peer.port + peer.host] = r_remote;

      if (ready) {
        r_remote['ready'](local_peer);
      } 

      if (reconcile) {
        reconcile = false;
        r_remote['sync'](local_peer, function() {
          server.emit('ready');

          r_remote['addPeer'](local_peer);            
          r_remote['addPeers'](connected_peers);

          syncRemotePeer(peer);
        });
      }
      else {
        r_remote['addPeer'](local_peer);
        r_remote['addPeers'](connected_peers);

        syncRemotePeer(peer);
      }
    });

    client.on('disconnect', function() {
      if (connected_peers.indexOf(peer) > -1) {
        debug('PEER DISCONNECTED @', peer);
        connected_peers.splice(connected_peers.indexOf(peer), 1)
      }
      if (ready_peers.indexOf(peer) > -1) {
        ready_peers.splice(ready_peers.indexOf(peer), 1);
      }
    });

    clients.push(client);
  };


  var server = rpc(methods);

  server.on('ready', function() {
    ready = true;
    debug('LOCAL READY', local_peer);
    connected_peers.forEach(function(peer) {
      var remote = connections[peer.port+peer.host];
      remote['ready'](local_peer, function(err) {});
    });
  });


  config.peers.forEach(methods.addPeer);
  loaded = true;
  

  function replicatePeers(op, cb) {
    debug('CONNECTED PEERS @', connected_peers);
    debug('READY PEERS @', ready_peers);

    var phase = 'quorum';
    var index = 0;

    if (ready_peers.length == 0)
      return cb(null, []);

    !function next() {
      var replicate_peers = ready_peers;
      if (config.mode == 'sync') {
        replicate_peers = config.peers;
      }
      
      replicate_peers.map(function(peer) {
        debug('COORDINATING PEER @', phase, peer.host, peer.port)

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

  function confirmPeerReplication(op, peers, cb) {

    if (peers.length == 0) return cb();

    var ops = [];
    peers.map(function(peer) {
      if (op.type == 'batch') {
        op.value.forEach(function(o) {
          ops.push({ type: 'del', key: prefixPeer(peer) + o.key })
        });
      }
      else {
        ops.push({ type: 'del', key: prefixPeer(peer) + op.key });
      }
    });

    db.batch(ops, cb);
  }
  
  function syncRemotePeer(peer) {
    debug('SYNC PEER EVENT @', local_peer)

    var remote_peer = connections[peer.port+peer.host];
    
    var ops = [];
    localdb.createReadStream({
      gte: prefixPeer(peer),
      lte: prefixPeer(peer) + '~'
    })
    .on('data', function(data) {
      ops.push({type: 'put', key: data.key.replace(prefixPeer(peer), ''), value: data.value});
    })
    .on('end', function() {
      if (ops.length == 0) return;
      
      var op = { type: 'batch', value: ops };
      remote_peer['commit'](op, peer, function(err) {
        if (err) return debug('SYNC PEER ERROR @', err);
        
        confirmPeerReplication(op, [peer], function(err) {
          if (err) return debug('SYNC CONFIRM PEER ERROR @', err);
        })
      });
    });
  }

  function replicate(op, cb) {
    debug('REPLICATION EVENT @', config.host, config.port)
    replicatePeers(op, function(err, peers) {
      if (err) return cb(err);
      methods.commit(op, local_peer, function(err) {
        if (err) return cb(err);
        confirmPeerReplication(op, peers, cb);
      });
    });
  }


  if (config.peers.length && config.peers.length > 0) {
    localdb.createReadStream()
      .on('data', function(data) {
        local_count++;
      })
      .on('end', function() {
        if (local_count == 0) {
          debug('NOT READY')
          reconcile = true;
        }
        else {
          server.emit('ready');
        }
      });
  }
  else {
    server.emit('ready');
  }

  return server;
}

exports.Server = Server;
exports.createServer = Server;

