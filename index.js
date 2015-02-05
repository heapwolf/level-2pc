var rpc = require('rpc-stream')
var createClient = require('./client')
var xtend = require('xtend')
var debug = require('debug')('level2pc')
var inherits = require('inherits')
var Emitter = require('events').EventEmitter

var prefix = '\xffxxl\xff'

var Replicator = module.exports = function Replicator(db, repl_opts) {

  if (!(this instanceof Replicator)) return new Replicator(db, repl_opts)
  Emitter.call(this)

  this._isReady = false;

  var that = this;
  var id = repl_opts.host + ':' + repl_opts.port
  var peers = {}
  var connections = {}
  var _db = {}

  _db.del = db.del.bind(db)
  _db.put = db.put.bind(db)
  _db.batch = db.batch.bind(db)
  _db.close = db.close.bind(db)


  db.addPeer = function addPeer(peer) {
    connect(peer)
  }


  db.quorum = function quorum(cb) {
    debug('QUORUM @%d [%j]', repl_opts.port)

    //
    // the purpose of the quorum step is to deterime if the 
    // database is capable of reposonding successfully to operations.
    //
    var testkey = prefix + prefix + 'q'
    _db.put(testkey, 1, cb)
  }


  db.commit = function commit(op, cb) {
    op.opts = op.opts || {};

    if (op.type == 'batch') {
      op.value.forEach(function(o) {
        op.value.push({ type: 'del', key: prefix + o.key })
      })
    }
    else {
      op.value = [
        { type: 'del', key: prefix + op.key },
        { 
          type: op.type,
          key: op.key,
          value: op.value,
          keyEncoding: op.keyEncoding || db.options.keyEncoding || 'utf8',
          valueEncoding: op.valueEncoding || db.options.valueEncoding || 'utf8'
        }
      ]
    }

    debug('COMMIT @%d [%j]', repl_opts.port, op.value)

    _db.batch(op.value, op.opts, cb)
  }


  db.del = function del(key, cb) {

    if (key.indexOf(prefix) == 0)
      return _db.del.apply(_db, arguments)

    queue(function() {
      _db.del(prefix + key, function(err) {
        if (err) return cb(err)
        var op = { type: 'del', key: key }
        replicate(op, cb)
      })
    })
  }


  db.put = function put(key, value, opts, cb) {

    if ('function' == typeof opts) {
      cb = opts
      opts = {}
    }

    queue(function() {
      _db.put(prefix + key, value, opts, function(err) {
        if (err) return cb(err)

        var op = {
          type: 'put',
          key: key,
          value: value,
          keyEncoding: opts.keyEncoding || db.options.keyEncoding || 'utf8',
          valueEncoding: opts.valueEncoding || db.options.valueEncoding || 'utf8'
        };

        replicate(op, cb)
      })
    })
  }


  db.batch = function batch(arr, opts, cb) {

    if (arr[0] && arr[0].key.indexOf(prefix) == 0)
      return _db.batch.apply(_db, arguments)

    if ('function' == typeof opts) {
      cb = opts
      opts = {}
    }

    queue(function() {
      _db.batch(prefixOps(arr), opts, function(err) {
        arr.map(function(a) {
          if (a.type == 'put') {
            a.keyEncoding = a.keyEncoding || opts.keyEncoding || 'utf8';
            a.valueEncoding = a.valueEncoding || opts.valueEncoding || 'utf8';
          }
        });

        var op = { type: 'batch', value: arr }
        replicate(op, cb)
      })
    })
  }


  function prefixWithPeer(peer) {
    return prefix + '!' + peer + '!'
  }


  function prefixOps(arr) {
    var ops = []
    arr.map(function opsMap(op) {
      ops.push({
        key: prefix + op.key,
        value: op.value,
        type: op.type,
        keyEncoding: 'utf8',
        valueEncoding: 'utf8'
      })
    })
    return ops
  }


  function quorumPhase(op, len, cb) {
    var index = 0
    var failures = 0

    for (var peer in peers) {

      peers[peer].quorum(function quorumCallback(err) {
        if (err && err.message == 'Database is not open') {

          debug('FAILURE EVENT @%s', peer)

          if (repl_opts.minConsensus && ++failures == repl_opts.minConsensus) {
            return cb(new Error('minimum consensus failed'))
          }
        }
        else if (err) {
          return cb(err)
        }

        if (++index == len) {
          commitPhase(op, len, cb)
        }
      })
    }
  }


  function commitPhase(op, len, cb) {
    var index = 0

    for (var peer in peers) {

      peers[peer].commit(op, function quorumCallback(err) {
        if (err && err.message != 'Database is not open') {
          return cb(err)
        }

        if (++index == len) {
          cb(null)
        }
      })
    }
  }


  function queue(fn) {
    if (that._isReady) fn()
    else that.once('ready', fn)
  }


  function replicate(op, cb) {

    debug('REPLICATION EVENT @%s', id)

    var len = repl_opts.minConsensus || Object.keys(peers).length
    if (!len || len == 0) return db.commit(op, cb)

    quorumPhase(op, len, function quorumPhaseCallback(err) {
      if (err) return cb(err)
      db.commit(op, cb)
    })
  }


  function ready(isReady) {
    if (isReady) debug('READY EVENT %s', id)
    else debug('NOT READY EVENT %s', id)
    that._isReady = isReady
    that.emit(isReady ? 'ready' : 'notready')
  }


  function connect(peer, index) {

    var peername = peer.host + ':' + peer.port

    if (typeof connections[peername] != 'undefined')
      return;

    var min = repl_opts.minConsensus || repl_opts.peers.length
    var client = createClient(repl_opts)

    client.connect(peer.port, peer.host)
    connections[peername] = client

    client.on('fail', function() {
      that.emit('fail', peer.host, peer.port)
    })

    client.on('reconnect', function() {
      debug('RECONNECT EVENT %s -> %s', id, peername)
      that.emit('reconnect', peer.host, peer.port)
    })

    client.on('connect', function onConnect(con) {
      debug('CONNECT EVENT %s -> %s', id,  peername)

      var client = rpc()
      var remote = client.wrap(db)
      client.pipe(con).pipe(client)
      peers[peername] = remote

      Object.keys(peers).forEach(function(p) {
        peers[p].addPeer({host: repl_opts.host, port: repl_opts.port});
      });

      that.emit('connect', peer.host, peer.port)
      if (!that._isReady && Object.keys(peers).length >= min) {
        ready(true)
      }
    })

    client.on('disconnect', function onDisconnect() {
      debug('DISCONNECT EVENT %s -> %s', id, peername)
      that.emit('disconnect', peer.host, peer.port)

      delete peers[peername];

      if (that._isReady && Object.keys(peers).length < min) {
        ready(false)
      }
    })
  }

  if (repl_opts.minConsensus == 0) {
    ready(true)
  }


  repl_opts.peers.forEach(connect)


  this.close = function closeServer() {
    for (var client in connections) {
      connections[client].disconnect()
    }
    peers = {}
  }


  this.createServer = function createServer() {
    return rpc(db)
  }
}

inherits(Replicator, Emitter)

