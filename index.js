var rpc = require('rpc-stream')
var createClient = require('./client')
var xtend = require('xtend')
var debug = require('debug')('level2pc')
var inherits = require('inherits')
var Emitter = require('events').EventEmitter
var Codec = require('level-codec')

var prefix = '\xffxxl\xff'

function Replicator(db, repl_opts) {

  if (!(this instanceof Replicator)) return new Replicator(db, repl_opts)
  Emitter.call(this)

  this._isReady = false

  var that = this
  var id = repl_opts.host + ':' + repl_opts.port
  var peers = {}
  var connections = {}
  var _db = {}

  _db.del = db.del.bind(db)
  _db.put = db.put.bind(db)
  _db.batch = db.batch.bind(db)
  _db.close = db.close.bind(db)

  // encode keys and values before sending pushing them through the rpc stream
  var codec = new Codec(db.options)

  var DEL = {}

  function encodeOp(key, value, opts) {
    var op = {}
    op.key = codec.encodeKey(key, opts)
    op.keyEncoding = codec.keyAsBuffer(opts) ? 'binary' : 'utf8'

    op.type = value === DEL ? 'del' : 'put'

    if (op.type == 'put') {
      op.value = codec.encodeValue(value, opts)
      op.valueEncoding = codec.valueAsBuffer(opts) ? 'binary' : 'utf8'
    }

    return op
  }

  function encodeOps(arr, opts) {
    // force utf8 or binary value encoding
    return codec.encodeBatch(arr, opts).map(function (op) {
      if (op.keyEncoding != 'binary') {
        op.keyEncoding = 'utf8'
      }
      if ('value' in op && op.valueEncoding != 'binary') {
        op.valueEncoding = 'utf8'
      }
      return op
    })
  }

  function encodePrefix(op) {
    if (typeof op.key === 'string') return prefix + op
    return new Buffer(prefix + op.key.toString('binary'), 'binary')
  }

  function prefixRemoveOp(op) {
    return {
      type: 'del',
      key: encodePrefix(op),
      keyEncoding: op.keyEncoding
    }
  }


  db.addPeer = function addPeer(peer) {
    connect(peer)
  }


  db.quorum = function quorum(cb) {
    debug('QUORUM @%d [%j]', repl_opts.port)

    //
    // the purpose of the quorum step is to determine if the
    // database is capable of reposonding successfully to operations.
    //
    var testkey = prefix + prefix + 'q'
    _db.put(testkey, 1, cb)
  }


  db.commit = function commit(op, cb) {
    var arr
    var opts
    if (op.type == 'batch') {
      opts = op.opts
      arr = op.value
      arr.forEach(function(o) {
        arr.push(prefixRemoveOp(o))
      })
    }
    else {
      opts = {}
      arr = [ prefixRemoveOp(op), op ]
    }

    debug('COMMIT @%d [%j]', repl_opts.port, arr)

    _db.batch(arr, opts, cb)
  }


  db.del = function del(key, opts, cb) {

    // TODO: is this really necessary?
    if (key.toString('binary').indexOf(prefix) == 0)
      return _db.del.apply(_db, arguments)

    if ('function' == typeof opts) {
      cb = opts
      opts = {}
    }

    var op
    queue(function() {
      try {
        op = encodeOp(key, DEL, opts)
      }
      catch (err) {
        return cb(err)
      }

      var _opts = { keyEncoding: op.keyEncoding }
      _db.del(encodePrefix(op), _opts, function(err) {
        if (err) return cb(err)

        replicate(op, cb)
      })
    })
  }


  db.put = function put(key, value, opts, cb) {

    if ('function' == typeof opts) {
      cb = opts
      opts = {}
    }

    var op
    queue(function() {
      try {
        op = encodeOp(key, value, opts)
      }
      catch (err) {
        return cb(err)
      }

      var _opts = { keyEncoding: op.keyEncoding }
      _db.put(encodePrefix(op), op.value, _opts, function (err) {
        if (err) return cb(err)

        replicate(op, cb)
      })
    })
  }


  db.batch = function batch(arr, opts, cb) {

    // TODO: is this really necessary?
    if (arr[0] && arr[0].key.toString('binary').indexOf(prefix) == 0)
      return _db.batch.apply(_db, arguments)

    if ('function' == typeof opts) {
      cb = opts
      opts = {}
    }

    var ops
    var prefixes
    queue(function() {
      try {
        ops = encodeOps(arr, opts)
        prefixed = ops.map(function (op) {
          return xtend(op, { key: encodePrefix(op) })
        })
      }
      catch (err) {
        return cb(err)
      }

      _db.batch(prefixed, opts, function (err) {
        replicate({ type: 'batch', value: ops, opts: opts }, cb)
      })
    })
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
    if (that._isReady) process.nextTick(fn)
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


  function connect(peer) {

    var peername = peer.host + ':' + peer.port

    if (connections === null || typeof connections[peername] != 'undefined')
      return

    var min = repl_opts.minConsensus || repl_opts.peers.length
    var client = createClient(repl_opts)
    var failures = 0

    client.connect(peer.port, peer.host)
    connections[peername] = client

    client.on('fail', function() {
      that.emit('fail', peer.host, peer.port)
    })

    client.on('error', function(err) {
      that.emit('error', err)
      if (failures++ == repl_opts.failAfter) {
        client.emit('fail')
      }
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
        peers[p].addPeer({host: repl_opts.host, port: repl_opts.port})
      })

      that.emit('connect', peer.host, peer.port)

      if (!that._isReady && Object.keys(peers).length >= min) {
        ready(true)
      }

    })

    client.on('disconnect', function onDisconnect() {
      debug('DISCONNECT EVENT %s -> %s', id, peername)
      that.emit('disconnect', peer.host, peer.port)

      delete peers[peername]

      if (repl_opts.minConsensus > 0
          && that._isReady
          && Object.keys(peers).length < min) {
        ready(false)
      }

    })
  }

  if (repl_opts.minConsensus == 0) {
    process.nextTick(ready.bind(null, true))
  }


  repl_opts.peers.forEach(connect)


  this.close = function closeServer() {
    var _connections = connections
    connections = null
    for (var client in _connections) {
      _connections[client].disconnect()
    }
    peers = {}
  }

  this.createServer = function createServer() {
    return rpc(db)
  }
}

inherits(Replicator, Emitter)

module.exports = Replicator

