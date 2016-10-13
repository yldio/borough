'use strict'

const Partition = require('./partition')

class Request {
  constructor (partition, body, borough) {
    this.partition = partition
    this.body = body
    this._borough = borough
  }

  otherPartition (partition) {
    return new Partition(partition, this._borough)
  }

}

module.exports = Request
