var inject = require('reconnect-core');

module.exports = function (opts) {

  opts = opts || { failAfter: 16 };

  var reconnect = inject(opts.transport || function() {
    return require('net').connect.apply(null, arguments);
  });

  return reconnect(opts);
};

