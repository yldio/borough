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
    debug('%s: topology updated, force = %j', this._subnode.id, force)
    if (force || this._subnode.is('leader')) {
      this._updateTopology()
    }

    this._maybeLeave(this._subnode.id) // maybe leave partition
  }

  _updateTopology () {
    debug('%s: _topology updated', this._subnode.id)
    if (this._updating) {
      debug('%s: already updating topology...', this._subnode.id)
      this._needsAnotherUpdate = true
    } else {
      this._updating = true
      this._needsAnotherUpdate = false
      const peerNodes = this._cluster.nodesForPartition(this._subnode.partition(), true)
      debug('%s: peer nodes are: %j', this._subnode.id, peerNodes)
      debug('%s: going to ensure partition on peer nodes %j', this._subnode.id, peerNodes)
      async.map(peerNodes, this._ensurePartition.bind(this), (err, peers) => {
        debug('%s: ensured partition on peer nodes %j, remote addresses are %j', this._subnode.id, peerNodes, peers)
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
    debug('%s: joining %s', this._subnode.id, peer)
    this._subnode.join(peer, done)
  }

  _maybeLeave (peer) {
    if (peer === this._subnode.id) {
      const me = this._cluster.whoami()
      const peerNodes = this._cluster.nodesForPartition(this._subnode.partition())
      debug('%s: peer nodes (including self): %j; self: %j', this._subnode.id, peerNodes, me)
      if (peerNodes.indexOf(me) < 0) {
        debug('%s: not part of partition %s: trying to leave self, starting quitter now..', this._subnode.id, this._subnode.partition())
        this._quitter.start()
      }
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
