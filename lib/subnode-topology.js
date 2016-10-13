'use strict'

const debug = require('debug')('borough:subnode:topology')
const async = require('async')
const EventEmitter = require('events')
const Quitter = require('./subnode-quitter')

class SubnodeTopology extends EventEmitter {

  constructor (subnode, cluster, options) {
    super()

    this._subnode = subnode
    this._cluster = cluster
    this._options = options

    this._updating = false
    this._needsAnotherUpdate = false
    this._peers = []

    this._quitter = new Quitter(subnode.partition(), subnode, cluster, options.quitter)
    this._quitter.on('warning', this.emit.bind(this, 'warning'))
  }

  topologyUpdated (force) {
    debug('topology updated, force = %j', force)
    if (force || this._subnode.is('leader')) {
      this._topologyUpdated()
    } else {
      debug('not leader, is', this._subnode._skiff._node._stateName)
    }
  }

  _topologyUpdated () {
    debug('_topology updated')
    if (this._updating) {
      debug('already updating topology...')
      this._needsAnotherUpdate = true
    } else {
      this._updating = true
      this._needsAnotherUpdate = false
      const peerNodes = this._cluster.nodesForPartition(this._subnode.partition(), true)
      debug('peer nodes are: %j', peerNodes)
      debug('going to ensure partition on peer nodes %j', peerNodes)
      async.map(peerNodes, this._ensurePartition.bind(this), (err, peers) => {
        debug('ensured partition on peer nodes %j, remote addresses are %j', peerNodes, peers)
        if (err) {
          this.emit('warning', err)
        } else {
          this.setPeers(peers.filter(p => !!p), err => {
            this._updating = false
            if (err) {
              this.emit('warning', err)
            }
            if (this._needsAnotherUpdate) {
              this.topologyUpdated()
            }
          })
        }
      })
    }
  }

  _ensurePartition (node, done) {
    this._cluster.ensureRemotePartition(this._subnode.partition(), node, done)
  }

  setPeers (peers, done) {
    // TODO: only join, don't process leaves here
    const leaves = this._peers.filter(p => peers.indexOf(p) < 0)
    const joins = peers.filter(p => this._peers.indexOf(p) < 0)
    async.each(
      joins,
      (peer, cb) => {
        this._join(peer, this._warningOnError(cb))
      },
      done)
    leaves.forEach(this._maybeLeave.bind(this))
  }

  _join (peer, done) {
    debug('joining %s', peer)
    this._subnode.join(peer, done)
  }

  _maybeLeave (peer) {
    debug('maybe leaving %s', peer)
    if (peer === this.id) {
      this._quitter.start()
    }
  }

  _warningOnError (cb) {
    return (err, result) => {
      if (err) {
        this.emit('warning', err)
      }
      if (cb) {
        cb(null, result)
      }
    }
  }
}

module.exports = SubnodeTopology
