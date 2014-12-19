var rpc = require('rpc-stream');
var createClient = require('./client');
var Hooks = require('level-hooks');
var net = require('net');
var ttl = require('level-ttl');

var prefix = '!!!';
var HR1 = { ttl: 1000 * 60 * 60 };

function Server(localdb, opts) {

  var batch = localdb.batch;
  var put = localdb.put;

  opts = opts || {};
  var methods = {};

  methods.quorum = function (key, value, cb) {
    put.call(localdb, prefix + key, value, HR1, cb);
  };
  
  methods.commit = function (key, value, cb) {
    batch.call(
      localdb,
      [
        { type: 'del', key: prefix + key },
        { type: 'put', key: key, value: value }
      ],
      cb
    );
  }; 

  var server = rpc(methods);

  server.peers = [];

  Hooks(ttl(localdb));

  function getQuorum(key, value, done) {

    var phase = 'quorum';
    var index = 0;

    !function connect() {
      server.peers.map(function(peer) {

        var client = createClient(opts);
        client.connect(peer.port, peer.host);

        client.on('connect', function(stream) {

          var client = rpc();
          var remote = client.wrap(methods);
          client.pipe(stream).pipe(client);

          remote[phase](key, value, function(err) {

            if (err) {
              return done(err);
            }

            if (++index == server.peers.length) {
              if (phase != 'quorum') {
                return done();
              }

              phase = 'commit';
              index = 0;
              connect();
            }
          });
        });
      });
    }();
  }

  localdb.hooks.pre({ start: prefix + '~' }, function (op, done) {

    localdb.put(prefix + op.key, op.value, function (err) {
      if (err) return done(err);

      getQuorum(op.key, op.value, function(err) {
        if (err) return done(err);
        methods.commit(op.key, op.value, done);
      });
    });
  });

  return server;
}

exports.Server = Server;
exports.createServer = Server;

