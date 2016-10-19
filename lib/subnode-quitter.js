'use strict'

const debug = require('debug')('borough:subnode:quitter')
const timers = require('timers')
const EventEmitter = require('events')
const async = require('async')

class SubnodeQuitter extends EventEmitter {
  constructor (partition, subnode, cluster, topology, options) {
    super()
    this._partition = partition
    this._subnode = subnode
    this._cluster = cluster
    this._topology = topology
    this._options = options
    this._timeout = null
    this._quitTimeout = null
  }

  start () {
    debug('(re)starting quitter')
    if (this._timeout) {
      timers.clearTimeout(this._timeout)
    }
    if (this._quitTimeout) {
      timers.clearTimeout(this._quitTimeout)
    }
    this.schedule()
  }

  schedule () {
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
    if (this._quitTimeout) {
      timers.clearTimeout(this._quitTimeout)
    }
  }

  _poll () {
    debug('%s: quitter poll..', this._subnode.id)
    this._timeout = null
    if (!this._partOfThePartition()) {
      debug('%s: not part of the partition %s', this._subnode.id, this._partition)
      this._partitionHealed(this._warningOnError(healed => {
        if (healed) {
          debug('%s: partition healed, leaving partition in %d ms..', this._subnode.id, this._partition, this._options.delayQuittingMS)

          // maximize availability by leaving the cluster a bit later in order for the
          // new node(s) to catch up
          this._quitTimeout = timers.setTimeout(() => {
            if (!this._partOfThePartition()) {
              console.log('%s: quitting partition now', this._subnode.id)
              this._topology.leaveSelf()
              this._subnode.leaveSelf(err => {
                if (err) {
                  debug('%s: error trying to leave self: ', this._subnode.id, err.message)
                  this.emit('warning', err)
                  debug('%s: going to retry later leaving partition %s...', this._subnode.id, this._partition)
                  this.schedule()
                } else {
                  debug('%s: successfully left partition %s', this._subnode.id, this._partition)
                }
              })
            }
          }, this._options.delayQuittingMS)
        } else {
          debug('%s: partition %s is not healed, going to retry later..', this._subnode.id, this._partition)
          // restart timer
          this.schedule()
        }
      }))
    }
  }

  _partOfThePartition () {
    const me = this._cluster.whoami()
    const nodes = this._cluster.nodesForPartition(this._partition)
    const amPartOfThePartition = (nodes.indexOf(me) >= 0)
    debug('%s: am I part of the partition? %j', this._subnode.id, amPartOfThePartition)
    return amPartOfThePartition
  }

  _partitionHealed (done) {
    const nodes = this._cluster.nodesForPartition(this._partition, this._options.redundancy, true)
    async.each(nodes, this._cluster.ping.bind(this._cluster), err => {
      debug('%s: partition healed: %j', this._subnode.id, !err)
      done(null, !err)
    })
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
