'use strict'

const debug = require('debug')('skiff:subnode:quitter')
const timers = require('timers')
const EventEmitter = require('events')
const async = require('async')

class SubnodeQuitter extends EventEmitter {
  constructor (partition, subnode, cluster, options) {
    super()
    this._partition = partition
    this._subnode = subnode
    this._cluster = cluster
    this._options = options
    this._timeout = null
  }

  start () {
    debug('(re)starting quitter')
    if (this._timeout) {
      timers.clearTimeout(this._timeout)
    }
    this._timeout = timers.setTimeout(this._poll.bind(this), this._options.pollTimeoutMS)
  }

  stop () {
    debug('stopping quitter..')
    if (this._timeout) {
      timers.clearTimeout(this._timeout)
    }
  }

  _poll () {
    this._timeout = null
    if (!this._partOfThePartition()) {
      this._partitionHealed(this._warningOnError(healed => {
        if (healed && !this._leader()) {
          this._leave()
        } else {
          // restart timer
          this.start()
        }
      }))
    }
  }

  _partOfThePartition () {
    const me = this._cluster.whoami()
    const nodes = this._cluster.nodesForPartition(this._partition, this._options.redundancy)
    return nodes.indexOf(me) >= 0
  }

  _partitionHealed (done) {
    const nodes = this._cluster.nodesForPartition(this._partition, this._options.redundancy, true)
    async.each(nodes, this._cluster.ping.bind(this._cluster), err => {
      done(null, !err)
    })
  }

  _leave () {
    this._subnode.leaveSelf()
  }

  _warningOnError (done) {
    const self = this
    return function (err, result) {
      if (err) {
        self.emit('warning', err)
      } else if (done) {
        done(result)
      }
    }
  }

}

module.exports = SubnodeQuitter
