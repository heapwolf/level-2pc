var inject = require('reconnect-core');

module.exports = function (opts) {

  opts = opts || { failAfter: 16 };

  var reconnect = inject(opts.transport || function() {
    return require('net').connect.apply(null, arguments);
  });

  var re = reconnect(opts, function (stream) {
    stream.on('error', re.emit.bind(re, 'error'));
  });

  return re;
};

