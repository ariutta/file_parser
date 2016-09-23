var env = require('node-env-file');
var humps = require('humps');
var Pgp = require('pg-promise');

env(__dirname + '/../.env');

var camelizeColumns = function(data) {
  var template = data[0];
  for (var prop in template) {
    var camel = humps.camelize(prop);
    if (!(camel in template)) {
      for (var i = 0; i < data.length; i++) {
        var d = data[i];
        d[camel] = d[prop];
        delete d[prop];
      }
    }
  }
};

var options = {
  capSQL: true,
  receive: function(data, result, e) {
    camelizeColumns(data);
  }
};

var pgp = Pgp(options);
var helpers = pgp.helpers;

/**
 * Convenience wrapper for pg-promise, which is a node wrapper for PostgreSQL
 */

var getDb = function(databaseUrl) {
  return pgp(databaseUrl || process.env.DATABASE_URL);
};

// Helper for linking to external query files:
var sql = function(file) {
  return new pgp.QueryFile(file, {minify: true});
};

module.exports = {
  as: pgp.as,
  getDb: getDb,
  ParameterizedQuery: pgp.ParameterizedQuery,
  sql: sql,
  helpers: helpers,
};
