var rpc = require('rpc-stream');
var createClient = require('./client');
var ttl = require('level-ttl');
var xtend = require('xtend');
var debug = require('debug');

var prefix = '\xffxxl\xff';
var ttlk = '\xffttl\xff';
var HR1 = { ttl: 1000 * 60 * 60 };

module.exports = function createServer(db, opts) {

  ttl(db);

  var id = opts.host + ':' + opts.port;
  var peers = {};
  var connections = {};
  var _db = {};

  _db.del = db.del.bind(db);
  _db.put = db.put.bind(db);
  _db.batch = db.batch.bind(db);
  _db.close = db.close.bind(db);


  db.addPeer = function addPeer(peer) {
    connect(peer);
  };


  db.quorum = function quorum(op, cb) {
    console.log('QUORUM @%d [%j]', opts.port, op);

    op.opts = op.opts || {};
    op.opts.ttl = opts.ttl || HR1;

    if (op.type == 'batch') {
      _db.batch(prefixOps(op.value), op.opts, cb);
    }
    else if (op.type == 'put') {
      _db.put(prefix + op.key, op.value, op.opts, cb);
    }
    else if (op.type == 'del') {
      _db.del(prefix + op.key, cb);
    }
  };


  db.commit = function commit(op, cb) {

    if (op.type == 'batch') {
      op.value.forEach(function(o) {
        op.value.push({ type: 'del', key: prefix + o.key });
      });
    }
    else {
      op.value = [
        { type: 'del', key: prefix + op.key },
        { type: op.type, key: op.key, value: op.value }
      ];
    }

    console.log('COMMIT @%d [%j]', opts.port, op.value);

    _db.batch(op.value, op.opts, cb);
  };


  db.del = function del(key, cb) {

    if (key.indexOf(ttlk) == 0)
      return del.apply(localdb, arguments);

    db.del(prefix + key, function(err) {
      if (err) return cb(err);
      var op = { type: 'del', key: key };
      replicate(op, cb);
    });
  };


  db.put = function put(key, value, opts, cb) {

    if ('function' == typeof opts) {
      cb = opts;
      opts = {};
    }

    _db.put(prefix + key, value, opts, function(err) {
      if (err) return cb(err);
      var op = { type: 'put', key: key, value: value, opts: opts };
      replicate(op, cb);
    });
  };


  db.batch = function batch(arr, opts, cb) {
    if (arr[0] && arr[0].key.indexOf(ttlk) == 0)
      return db.batch.apply(localdb, arguments);

    if ('function' == typeof opts) {
      cb = opts;
      opts = {};
    }

    _db.batch(prefixOps(arr, true), opts, function(err) {
      var op = { type: 'batch', value: arr };
      replicate(op, cb);
    });
  };


  db.close = function close() {
    for (var client in connections) {
      connections[client].disconnect();
      delete connections[client];
    }
    _db.close.apply(_db, arguments);
  };


  function prefixWithPeer(peer) {
    return prefix + '!' + peer + '!';
  }


  function prefixOps(arr) {
    var ops = [];
    arr.map(function opsMap(op) {
      ops.push({ 
        key: prefix + op.key, 
        value: op.value, 
        type: op.type 
      });
    });
    return ops;
  }


  function quorumPhase(op, len, cb) {
    var index = 0;
    var failures = 0;

    for (var peer in peers) {

      peers[peer].quorum(op, function quorumCallback(err) {
        if (err && err.message == 'Database is not open') {

          console.log('FAILURE EVENT @%s', peer);

          if (opts.minConcensus && ++failures == opts.minConcensus) {
            return cb(new Error('minimum concensus failed'));
          }
        }
        else if (err) {
          return cb(err);
        }

        if (++index == len) {
          commitPhase(op, len, cb);
        }
      });
    }
  }


  function commitPhase(op, len, cb) {
    var index = 0;

    for (var peer in peers) {

      peers[peer].commit(op, function quorumCallback(err) {
        if (err && err.message != 'Database is not open') {
          return cb(err);
        }

        if (++index == len) {
          cb(null);
        }
      });
    }
  }


  function replicate(op, cb) {

    console.log('REPLICATION EVENT @%s', opts.port)

    var len = Object.keys(peers).length;
    if (!len) return cb(null);
    quorumPhase(op, len, function quorumPhaseCallback(err) {
      if (err) return cb(err);
      db.commit(op, cb);
    });
  }


  function connect(peer, index) {

    var peername = peer.host + ':' + peer.port;
    var client = createClient(opts);

    client.connect(peer.port, peer.host);
    connections[peername] = client;

    console.log('CONNECT EVENT %s -> %s', id,  peername)

    client.on('connect', function onConnect(s) {

      console.log('CONNECTION EVENT %s -> %s', id,  peername)

      var r = rpc();
      remote = r.wrap(db);
      r.pipe(s).pipe(r);
      peers[peername] = remote;
    });

    client.on('disconnect', function onDisconnect() {
      delete peers[peername];
      delete connections[peername];
    });
  }

  opts.peers.forEach(connect);

  return function Server(con) {
    return rpc(db);
  };
};

