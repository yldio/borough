'use strict'

class Partition {
  constructor (name, borough) {
    this.name = name
    this._borough = borough
  }

  get (key, done) {
    this._borough.remoteCommand(this.name, { type: 'get', key }, done)
  }

  put (key, value, done) {
    this._borough.remoteCommand(this.name, { type: 'put', key, value }, done)
  }

  info (done) {
    this._borough.remoteCommand(this.name, 'info', done)
  }
}

module.exports = Partition
