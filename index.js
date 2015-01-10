var rpc = require('rpc-stream');
var createClient = require('./client');
var net = require('net');
var ttl = require('level-ttl');
var debug = require('debug')('level2pc');
var multilevel = require('multilevel');

var prefix = '\xffxxl\xff';
var ttlk = '\xffttl\xff';
var HR1 = { ttl: 1000 * 60 * 60 };


function Server(localdb, config) {
 
  config = config || {};
  ttl(localdb);

  var ready, loaded, reconcile;
  var ready_peers = [];
  var clients = [];
  var connected_peers = [];
  var connections = {};
  var streams = [];
  var local_count = 0;
  var local_peer  = {host: config.host, port: config.port};


  // Setup and overload leveldb functions
  localdb._repl = {
    batch: localdb.batch.bind(localdb),
    put: localdb.put.bind(localdb),
    del: localdb.del.bind(localdb),
    close: localdb.close.bind(localdb)
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
  
  function peerIndex(peers, peer) {
    if (!containsPeer(peers, peer)) return -1;
    for (var i=0; i<peers.length; i++) {
      if (peers[i].host == peer.host && peers[i].port == peer.port) {
        return i;
      }
    }
  }

  function prefixPeer(peer) {
    return prefix + peer.port + peer.host + '\xff';
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

  function del(db, key, cb) {

    if (key.indexOf(ttlk) == 0)
      return del.apply(localdb, arguments);

    var ops = [{ type: 'del', key: prefixPeer(local_peer) + key }];
    config.peers.forEach(function(peer) {
      ops.push({ type: 'del', key: prefixPeer(peer) + key })
    });

    db._repl.batch.call(db, ops, function localdbDeleteBatchCallback(err) {
      if (err) return cb(err);
      var op = { type: 'del', key: key };
      replicate(op, cb);
    });
  };

  function put(db, key, val, opts, cb) {

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

    db._repl.batch.call(db, ops, function localdbPutBatchCallback(err) {
      if (err) return cb(err);
      var op = { type: 'put', key: key, value: val, opts: opts };
      replicate(op, cb);
    });
  };

  function batch(db, arr, opts, cb) {
    if (arr[0] && arr[0].key.indexOf(ttlk) == 0)
      return db.batch.apply(localdb, arguments);

    if ('function' == typeof opts) {
      cb = opts;
      opts = {};
    }

    db._repl.batch.call(db, prefixOps(arr, true), opts, function localdbBatchBatchCallback(err) {
      var op = { type: 'batch', value: arr };
      replicate(op, cb);
    });
  };
  
  function close(db, cb) {
    clients.map(function(client) {
      client.disconnect();
    });
    return db._repl.close.call(db, cb);
  };
  
  localdb['put']   = put.bind(null, localdb);
  localdb['del']   = del.bind(null, localdb);
  localdb['batch'] = batch.bind(null, localdb);
  localdb['close'] = close.bind(null, localdb);

  localdb.methods = localdb.methods || {};
  localdb.methods['quorum'] = { type: 'async' };
  localdb.methods['commit'] = { type: 'async' };
  localdb.methods['ready']  = { type: 'async' };
  localdb.methods['addpeer']= { type: 'async' };
  localdb.methods['confirm']= { type: 'async' };

  localdb['quorum'] = function (op, cb) {
    debug('QUORUM PHASE @', config.host, config.port)
    
    if (op.type == 'batch') {
      localdb._repl.batch(prefixOps(op.value, false), op.opts, cb);
    }
    else if (op.type == 'put') {
      localdb._repl.put(prefixPeer(local_peer) + op.key, op.value, op.opts, cb);
    }
    else if (op.type == 'del') {
      localdb._repl.del(prefixPeer(local_peer) + op.key, cb);
    }
  };
  
  localdb['commit'] = function (op, cb) {
    debug('COMMIT PHASE @', config.host, config.port)

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

    op.opts = op.opts || { ttl: config.ttl };

    localdb._repl.batch(op.value, op.opts, cb);
  };
  
  localdb['confirm'] = function(key, cb) {
    debug('CONFIRM SYNC', key);
    localdb._repl.del(key, cb);
  }
  
  localdb['ready'] = function(peer) {
    debug('PEER READY', peer.port, peer.host);
    if (!containsPeer(ready_peers, peer)) {
      ready_peers.push(peer); 
    }
  };

  localdb['addpeer'] = function(peer) {
    addPeer(peer);
  };

  //multilevel.writeManifest(localdb, __dirname + '/manifest.json');

  var server = multilevel.server(localdb);

  server.on('error', function serverErrorHandler(err) {});

  server.on('ready', function serverReadyHandler() {
    debug('READY')
    debug('Ready Peers', ready_peers);
    ready = true;
    connected_peers.forEach(function(peer) {
      var remote = connections[peer.port+peer.host];
      remote.ready(local_peer);
    });
  });

  server.on('close', function serverCloseHandler() {
    // Close any open read streams
    streams.map(function(stream) {
      stream.pause();
      stream.destroy();
    });

    // Close out all connected peers
    clients.map(function(client) {
      client.disconnect();
    });
  });


  function addPeer(peer) {
    if (isLocalPeer(peer)) {
      return debug('addPeer.isLocalPeer', peer)
    }
    if (loaded && containsPeer(config.peers, peer)) {
      return debug('addPeer.alreadyConfigured', peer);
    }

    debug('addPeer', peer);

    config.peers.push(peer);

    var db = multilevel.client(require('./manifest.json'));      
    connections[peer.port+peer.host] = db;
    db.on('error', function(err) {
      debug('MultiLevel Client Error', err);
    });

    var cl = createClient(config);
    var cn = cl.connect(peer.port, peer.host);

    clients.push(cl);

    cl.on('error', function(err) {
      debug('Peer Error', peer.port, peer.host, err);
      server.emit('error', err);
    });
    cl.on('fail', function() {
      debug('Peer Fail', peer.port, peer.host);
      server.emit('error', connectionError(peer.host, peer.port));
    });
    cl.on('reconnect', function() {
      debug('Peer Reconnect Try', peer.host, peer.port);
    })
    cl.on('connect', function(con) {
      debug('Peer Connected', peer.port, peer.host);

      con.pipe(db.createRpcStream()).pipe(con)
      
      connected_peers.push(peer);

      var remote = connections[peer.port+peer.host];

      remote.addpeer(local_peer);
      config.peers.map(function(p) {
        remote.addpeer(p);
      });

      if (ready)
        remote.ready(local_peer);

      // Pull all from first connected peer!
      if (reconcile) {
        debug('Reconciling');
        reconcile = false;
        var reconcile_count = 0;
        var rstream = remote.createReadStream()
          .on('data', function(data) {
            if (data.key.indexOf(prefix) == 0) return;
            reconcile_count++;
            localdb._repl.put(data.key, data.value);
          })
          .on('error', function(err) {
            debug('Reconciliation ReadStream Error', err);
          })
          .on('close', function() {
            debug('Reconciliation Complete, Records Syncd', reconcile_count);
            
            cleanupStream(rstream);
            
            if (!ready)
              server.emit('ready');
          })

        streams.push(rstream);
      }
      else {
        debug('Syncing Missing Keys ...')
        var sync_count = 0;
        var rstream = remote.createReadStream({
          gte: prefixPeer(local_peer),
          lte: prefixPeer(local_peer) + '~'
        })
          .on('data', function(data) {
            sync_count++;
            localdb._repl.put(data.key.replace(prefixPeer(local_peer), ''), data.value, function(err) {
              remote.confirm(data.key);
            });
          })
          .on('error', function(err) {
            debug('Sync Missing Keys ReadStream Error', err);
          })
          .on('close', function() {
            debug('Sync Missing keys Complete, Records Syncd', sync_count);

            cleanupStream(rstream);

            if (!ready)
              server.emit('ready');
          });

        streams.push(rstream);
      }
    });
    
    cl.on('disconnect', function() {
      if (containsPeer(connected_peers, peer)) {
        debug('Removed from connected_peers array');
        connected_peers.splice(peerIndex(connected_peers, peer), 1)
      }
      if (containsPeer(ready_peers, peer)) {
        debug('Removed from ready_peers array');
        ready_peers.splice(peerIndex(ready_peers, peer), 1);
      }
    });
  };


  function replicatePeers(op, cb) {

    var phase = 'quorum';
    var index = 0;
    
    var replicate_peers = ready_peers;
    
    if (replicate_peers.length == 0) return cb(null, []);
    
    !function next() {
      replicate_peers.map(function(peer) {
        debug('COORDINATING PEER @', config.host, config.port, peer);
        debug('REPLICATING PEERS @', replicate_peers);

        var remote = connections[peer.port + peer.host];

        function write() {
          debug('WRITE PHASE');
          remote[phase](op, function(err) {
            if (err) {
              return cb(err);
            }

            if (++index == replicate_peers.length) {
              if (phase != 'quorum') {
                return cb(null, replicate_peers);
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
    debug('REPLICATION EVENT @', config.port, config.host)
    replicatePeers(op, function replicateReplicatePeersCallback(err, replicate_peers) {
      if (err) return cb(err);
      confirmReplication(op, replicate_peers, cb);
    });
  }

  function confirmReplication(op, peers, cb) {
    if (peers.length == 0) {
      debug('REPLICATION EVENT CONFIRMED @', config.port, config.host);
      return cb();
    }

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

    ops.push({ type: 'del', key: prefixPeer(local_peer) + op.key })

    if (op.type == 'batch') {
      op.value.forEach(function(o) {
        ops.push(o);
      })
    } else {
      ops.push(op)
    }

    localdb._repl.batch(ops, function(err) {
      if (err) return cb(err);
      debug('REPLICATION EVENT CONFIRMED @', config.port, config.host);
      return cb();
    });
  }

  function cleanupStream(s) {
    for (var i=0; i<streams.length; i++) {
      if (streams[i] == s)
        streams.splice(i, 1);
    }
  }

  config.peers = config.peers || [];
  config.peers.forEach(addPeer);
  loaded = true;

  if (config.peers.length && config.peers.length > 0) {
    var local_count = 0;
    localdb.createReadStream({ limit: 5 })
      .on('data', function(data) { local_count++; })
      .on('close', function() {
        if (local_count == 0) {
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
  
  /** might need this, might not
  localdb.createKeyStream({
    gte: prefixPeer(local_peer),
    lte: prefixPeer(local_peer) + '~'
  })
  .on('data', function(key) {
    localdb._repl.del(key);
  })
  .on('close', function() {
    debug('Local Peer Replication Cleanup Complete')
  })*/

  return server;
}

exports.Server = Server;
exports.createServer = Server;
