var inject = require('reconnect-core');

module.exports = function (methods, transport) {

  var reconnect = inject(transport || function() {
    return require('net').connect.apply(null, arguments);
  });

  return reconnect({ failAfter: 10 });
};

