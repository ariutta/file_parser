var _ = require('lodash');
var fs = require('fs');
var parse = require('csv-parse');
var path = require('path');
var pg = require('pg');
var pgp = require('./pg-promise-wrapper.js');
var Rx = require('rx');
var RxNode = require('rx-node');
var split = require('split');

module.exports = function(dataDir, specsDir, databaseUrl) {
  dataDir = dataDir || path.join(__dirname, '..', 'data');
  specsDir = specsDir || path.join(__dirname, '..', 'specs');

  var db = pgp.getDb(databaseUrl);
  var PQ = pgp.ParameterizedQuery;

  var datatypeParsersByDatatype = {
    'TEXT': function(x) {
      // TODO check assumption: trimming
      // leading/trailing whitespace is
      // appropriate.
      return _.trim(x);
    },
    'BOOLEAN': function(x) {
      if (!_.isNaN(parseInt(x))) {
        return Boolean(parseInt(x));
      } else {
        return Boolean(x);
      }
    },
    'INTEGER': parseInt,
    // TODO check whether we might encounter other datatypes,
    // such as dates, addresses, currency, etc.
  };

  function createTable(format, spec) {
    // TODO check whether there are appropriate constraints to
    // add, such as a UNIQUE constraint to avoid duplicates if
    // a user accidentally drags the same file into the data/
    // directory twice.
    // TODO check whether there are appropriate relations to
    // model (foreign keys) and whether we should normalize
    // any data.
    // TODO what are the chances of getting a name collision where
    // a data file is already using column name "id" or "dropped"?
    var columnsSpec = [
      'id serial PRIMARY KEY',
      'dropped date'
    ].concat(
        spec
        .map(function(columnSpec) {
          return [columnSpec.columnName, columnSpec.datatype].join(' ');
        })
    )
    .join(', ');
    // TODO check assumption: spec files can be trusted (SQL injection).
    var sqlCreateTable = 'CREATE TABLE IF NOT EXISTS ' + format + ' (' + columnsSpec + ')';
    var createTablePQ = new PQ(sqlCreateTable);
    return Rx.Observable.fromPromise(db.none(createTablePQ));
  }

  function insertData(dataEnvelope) {
    var dropped = dataEnvelope.dropped;
    var format = dataEnvelope.format;
    var data = dataEnvelope.data
    .map(function(x) {
      x.dropped = dropped;
      return x;
    });
    // TODO check assumption: all rows must have the same fields
    var columns = _.keys(data[0]);
    var sqlInsertDataMulti = pgp.helpers.insert(data, columns, format);
    return Rx.Observable.fromPromise(db.none(sqlInsertDataMulti));
  }

  function loadDataFile(filename) {
    var dataStream = fs.createReadStream(path.join(dataDir, filename));
    var dataFileNameParsed = parseNameForDataFile(filename);
    var format = dataFileNameParsed.format;
    // TODO check assumption: spec file will always be present before
    // new data file is dropped.
    return loadSpecFile(format + '.csv')
    .flatMap(function(spec) {
      return parseDataFile(dataStream, spec)
      .toArray()
      .map(function(parsedData) {
        return {
          dropped: dataFileNameParsed.dropped,
          format: dataFileNameParsed.format,
          data: parsedData,
          spec: spec,
        };
      });
    })
    .flatMap(function(dataEnvelope) {
      return insertData(dataEnvelope);
    });
  }

  function loadSpecFile(filename) {
    var format = filename.slice(0, -4);
    return parseSpecFile(filename)
    .flatMap(function(spec) {
      return createTable(format, spec)
      .map(function() {
        return spec;
      });
    });
  }

  function parseDataFile(dataStream, spec) {
    return RxNode.fromReadableStream(dataStream.pipe(split()))
    .filter(function(row) {
      return row !== '';
    })
    .flatMap(function(row) {
      var begin = 0;
      return Rx.Observable.from(spec)
      .reduce(function(acc, columnSpec) {
        var columnName = columnSpec.columnName;
        var width = columnSpec.width;
        var datatype = columnSpec.datatype;
        // TODO check whether we might need to handle missing field(s)
        var rawField = row.slice(begin, begin + width);
        // NOTE: side-effect
        begin += width;
        var parsedField = datatypeParsersByDatatype[datatype](rawField);
        acc[columnName] = parsedField;
        return acc;
      }, {});
    });
  }

  function parseNameForDataFile(filename) {
    var components = filename.slice(0, -4).split('_');
    return {
      dropped: components[1],
      format: components[0]
    };
  }

  function parseSpecFile(filename) {
    var input = fs.createReadStream(path.join(specsDir, filename));

    return RxNode.fromReadableStream(input.pipe(parse({
      columns: true,
      skip_empty_lines: true
    })))
    .map(function(data) {
      data.columnName = data['column name'];
      delete data['column name'];
      return data;
    })
    .map(function(data) {
      data.width = parseFloat(data.width);
      return data;
    })
    .toArray();
  }

  function start() {
    fs.watch(dataDir, function(eventType, filename) {
      // TODO what types of events do we need to handle?
      // current assumption: data files are never duplicates and are only added
      if (eventType !== 'rename') {
        throw new Error('fixed2sql watcher: cannot handle eventType "' + eventType + '"');
      }
      if (!filename) {
        throw new Error('fixed2sql watcher: filename missing');
      }
      loadDataFile(filename)
      .subscribe(function(result) {
        console.log('Successfully loaded data from "' + filename + '"');
      }, function(err) {
        throw err;
      });
    });
  }

  return {
    loadDataFile: loadDataFile,
    start: start,
  };
};
