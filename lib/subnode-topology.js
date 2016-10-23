'use strict'

const debug = require('debug')('borough:subnode:topology')
const async = require('async')
const EventEmitter = require('events')
const timers = require('timers')

class SubnodeTopology extends EventEmitter {

  constructor (subnode, cluster, options) {
    super()

    this._subnode = subnode
    this._cluster = cluster
    this._options = options

    this._needsAnotherUpdate = false
    this._peers = (options.peers || [])

    this._updating = false
    this._needsUpdate = false

    // this._interval = timers.setInterval(this._maybeLeave.bind(this, this._subnode.id), this._options.maybeQuitIntervalMS)

  }

  start () {
    // nothing to start
  }

  stop () {
    // nothing to stop
  }

  topologyUpdated (force) {
    debug('%s: topology updated, force = %j', this._subnode.id, force)
    if (this._subnode.is('leader')) {
      this._updateTopology()
    }
  }

  _updateTopology () {
    debug('%s: update topology', this._subnode.id)
    const self = this

    if (this._updating) {
      this._needsUpdate = true
    } else {
      this._updating = true
      this._needsUpdate = false

      const peerNodes = this._cluster.nodesForPartition(this._subnode.partition())
      debug('%s: peer nodes are: %j', this._subnode.id, peerNodes)
      debug('%s: going to ensure partition on peer nodes %j', this._subnode.id, peerNodes)

      async.map(peerNodes, this._ensurePartition.bind(this), (err, peers) => {
        debug('%s: ensured partition on peer nodes %j, remote addresses are %j', this._subnode.id, peerNodes, peers)
        if (err) {
          handleError(err)
        } else {
          debug('%s: partition subnode peers are: %j', this._subnode.id, peers)
          this._setPeers(peers, err => {
            this._updating = false
            if (err) {
              handleError(err)
            }
            if (this._needsUpdate) {
              this._updateTopology()
            }
          })
        }
      })
    }

    function handleError(err) {
      self._updating = false
      self._needsAnotherUpdate = true
      self._retryLater()
      self.emit('warning', err)
    }
  }

  _setPeers (peers, done) {
    // TODO: only join, don't process leaves here
    const changes = this._calculateChanges(peers)
    this._applyChanges(changes, done)
  }

  _calculateChanges(peers) {
    let changes = []

    const joins = peers.filter(p => this._peers.indexOf(p) < 0)
    debug('%s: joins: %j', this._subnode.id, joins)
    changes = changes.concat(joins.map(peer => {
      return {type: 'join', peer}
    }))

    const leaves = this._peers.filter(p => peers.indexOf(p) < 0)
    debug('%s: leaves: %j', this._subnode.id, leaves)
    changes = changes.concat(leaves.map(peer => {
      return {type: 'leave', peer}
    }))

    return changes
  }

  _applyChanges (changes, done) {
    debug('%s: applying changes: %j', this._subnode.id, changes)
    async.eachSeries(changes, this._applyChange.bind(this), done)
  }

  _applyChange (change, done) {
    switch (change.type) {
      case 'leave':
        this._applyLeave(change.peer, done)
        break
      case 'join':
        this._applyJoin(change.peer, done)
        break
    }
  }

  _applyLeave (peer, done) {
    debug('%s: going to apply leave to %s', this._subnode.id, peer)
    this._subnode.leave(peer, done)
  }

  _applyJoin (peer, done) {
    this._subnode.join(peer, done)
  }

  peerJoined (peer) {
    debug('%s: peer %s has joined', this._subnode.id, peer)
    if ((this._peers.indexOf(peer) < 0)) {
      this._peers = this._peers.concat(peer)
    }
  }

  peerLeft (peer) {
    debug('%s: peer %s left', this._subnode.id, peer)
    this._peers = this._peers.filter(p => p !== peer)
    if (peer === this._subnode.id) {
      this._subnode.leftSelf(this._warningOnError())
    }
  }

  _retryLater () {
    timers.setTimeout(this._updateTopology.bind(this), this._options.retryMS)
  }

  _ensurePartition (node, done) {
    this._cluster.ensureRemotePartition(
      this._subnode.partition(),
      node,
      this._peers,
      done)
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
