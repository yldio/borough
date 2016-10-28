'use strict'

const debug = require('debug')('borough:partition')
const AbstractLevelDown = require('abstract-leveldown').AbstractLevelDOWN

class Partition extends AbstractLevelDown {
  constructor (name, borough) {
    super(name)
    this.name = name
    this._borough = borough
  }

  info (done) {
    this._borough.remoteCommand(this.name, 'info', done)
  }

  // AbstractLevelDown

  _close (done) {
    // do nothing
    process.nextTick(done)
  }

  _get (key, options, done) {
    debug('get %j', key)
    this._borough.remoteCommand(this.name, { type: 'get', key }, done)
  }

  _put (key, value, options, done) {
    debug('put %j, %j', key, value)
    this._borough.remoteCommand(this.name, { type: 'put', key, value }, done)
  }

  _del (key, options, done) {
    debug('del %j', key)
    this._borough.remoteCommand(this.name, { type: 'del', key }, done)
  }

  _batch (array, options, done) {
    debug('batch %j', array)
    this._borough.remoteCommand(this.name, { type: 'batch', array }, done)
  }

  _iterator (options) {
    return this._node.iterator(options)
  }

}

module.exports = Partition
