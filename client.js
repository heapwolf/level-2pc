var inject = require('reconnect-core');

module.exports = function (opts, transport) {

  opts = opts || { failAfter: 10 };

  var reconnect = inject(transport || function() {
    return require('net').connect.apply(null, arguments);
  });

  return reconnect(opts);
};

