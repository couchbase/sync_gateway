var core = require("./hoax-core"),
  request = require("browser-request");

request.log.debug = function() {};

module.exports = core(request);
