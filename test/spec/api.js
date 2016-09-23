/**
 * Test public APIs
 */

var env = require('node-env-file');
var expect = require('chai').expect;
var path = require('path');
var Rx = require('rx');
var sinon = require('sinon');

var dataDir = path.join(__dirname, '..', 'inputs', 'data');
var specsDir = path.join(__dirname, '..', 'inputs', 'specs');
var fixed2sql = require('../../index')(dataDir, specsDir, process.env.TEST_DATABASE_URL);

var pg = require('pg');
var pgp = require('../../lib/pg-promise-wrapper.js');

var db = pgp.getDb(process.env.TEST_DATABASE_URL);

// Run tests
describe('Public API', function() {

  describe('loadDataFile', function() {

    it('should add records to the db', function(done) {
      Rx.Observable.fromPromise(db.none('DROP TABLE IF EXISTS testformat1;'))
      .flatMap(function() {
        return fixed2sql.loadDataFile('testformat1_2015-06-28.txt');
      })
      .flatMap(function() {
        return Rx.Observable.fromPromise(db.many('SELECT * FROM testformat1;'));
      })
      .subscribe(function(actual) {
        var expected = [{
          'id': 1,
          'dropped': '2015-06-28T07:00:00.000Z',
          'name': 'Foonyor',
          'valid': true,
          'count': 1
        }, {
          'id': 2,
          'dropped': '2015-06-28T07:00:00.000Z',
          'name': 'Barzane',
          'valid': false,
          'count': -12
        }, {
          'id': 3,
          'dropped': '2015-06-28T07:00:00.000Z',
          'name': 'Quuxitude',
          'valid': true,
          'count': 103
        }];
        expect(JSON.stringify(actual)).to.equal(JSON.stringify(expected));
        done();
      }, done);
    });

  });

});
