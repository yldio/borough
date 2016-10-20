'use strict'

const debug = require('debug')('borough:subnode:quitter')
const EventEmitter = require('events')
const async = require('async')
const timers = require('timers')

class SubnodeQuitter extends EventEmitter {
  constructor (partition, subnode, cluster, topology, options) {
    super()
    this._partition = partition
    this._subnode = subnode
    this._cluster = cluster
    this._topology = topology
    this._options = options
  }

  start () {
    debug('(re)starting quitter')
    console.log('%s: starting quitter...', this._subnode.id)
    this._listener = this._poll.bind(this)
    this.on('topology committed', this._listener)
  }

  stop () {
    debug('stopping quitter..')
    this.removeListener('topology committed', this._listener)
  }

  topologyCommitted() {
    console.log('\n\n\n\n    -> %s: QUITTER: topology committed...', this._subnode.id)
    this.emit('topology committed')
  }

  _poll () {
    console.log('\n      -> %s: quitter poll..', this._subnode.id)
    debug('%s: quitter poll..', this._subnode.id)
    this._timeout = null
    if (!this._partOfThePartition()) {
      debug('%s: not part of the partition %s', this._subnode.id, this._partition)
      this._partitionHealed(this._warningOnError(healed => {
        if (healed) {
          debug('%s: partition healed, leaving partition', this._subnode.id, this._partition)
          if (!this._partOfThePartition()) {
            console.log('%s: quitting partition now', this._subnode.id)
            this._topology.leaveSelf()
            this._subnode.leaveSelf(err => {
              if (err) {
                debug('%s: error trying to leave self: ', this._subnode.id, err.message)
                this.emit('warning', err)
                debug('%s: going to retry later leaving partition %s...', this._subnode.id, this._partition)
              } else {
                debug('%s: successfully left partition %s', this._subnode.id, this._partition)
              }
            })
          }
        } else {
          debug('%s: partition %s is not healed, going to retry later..', this._subnode.id, this._partition)
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
