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

    this._quitter = new Quitter(subnode.partition(), subnode, cluster, this, options.quitter)
    this._quitter.on('warning', this.emit.bind(this, 'warning'))

    this.on('joined', this._onPeerJoined)
    this.on('left', this._onPeerLeft)
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
      console.log('%s: already updating...', this._subnode.id)
      debug('%s: already updating topology...', this._subnode.id)
      this._needsAnotherUpdate = true
    } else {
      debug('%s: update topology', this._subnode.id)
      console.log('%s: update topology', this._subnode.id)
      debug('%s: my state is %s', this._subnode.id, this._subnode._skiff._node._stateName)
      console.log('%s: my state is %s', this._subnode.id, this._subnode._skiff._node._stateName)

      this._updating = true
      this._needsAnotherUpdate = false
      const peerNodes = this._cluster.nodesForPartition(this._subnode.partition(), true)
      debug('%s: peer nodes are: %j', this._subnode.id, peerNodes)
      debug('%s: going to ensure partition on peer nodes %j', this._subnode.id, peerNodes)
      console.log('%s: going to ensure partitions..', this._subnode.id)
      async.map(peerNodes, this._ensurePartition.bind(this), (err, peers) => {
        console.log('%s: ensured partition on peer nodes %j, remote addresses are %j', this._subnode.id, peerNodes, peers)
        debug('%s: ensured partition on peer nodes %j, remote addresses are %j', this._subnode.id, peerNodes, peers)
        if (err) {
          this._updating = false
          this._needsAnotherUpdate = true
          this.emit('warning', err)
        } else {
          debug('%s: partition subnode peers are: %j', this._subnode.id, peers)
          console.log('%s: partition subnode peers are: %j', this._subnode.id, peers)
          this._setPeers(peers.filter(p => !!p), err => {
            this._updating = false
            if (err) {
              this._needsAnotherUpdate = true
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
    this._cluster.ensureRemotePartition(
      this._subnode.partition(),
      node,
      (err, remoteAddress) => {
        if (err) {
          done(err)
        } else {
          if (this._peers.indexOf(remoteAddress) < 0) {
            this._join(remoteAddress, err => {
              if (err) {
                done(err)
              } else {
                done(err, remoteAddress)
              }
            })
          } else {
            done(err, remoteAddress)
          }
        }
      })
  }

  _setPeers (peers, done) {
    // TODO: only join, don't process leaves here
    const leaves = this._peers.filter(p => peers.indexOf(p) < 0)
    debug('%s: leaves: %j', this._subnode.id, leaves)
    console.log('%s: leaves: %j', this._subnode.id, leaves)
    const joins = peers.filter(p => this._peers.indexOf(p) < 0)
    console.log('%s: joins: %j', this._subnode.id, joins)
    async.eachSeries(
      joins,
      (peer, cb) => {
        this._join(peer, this._warningOnError(cb))
      },
      err => {
        if (err) {
          done(err)
        } else {
          leaves.forEach(this._maybeLeave.bind(this))
          done()
        }
      })
  }

  _join (peer, done) {
    debug('%s: joining %s', this._subnode.id, peer)
    console.log('%s: joining %s', this._subnode.id, peer)
    this._subnode.join(peer, err => {
      console.log('%s: joined %s', this._subnode.id, peer)
      if (err) {
        debug(
          '%s: error joining %s to partition %s: %s',
          this._subnode.id,
          peer,
          this._subnode.partition(),
          err.stack)
      } else {
        debug(
          '%s: successfully joined %s to partition %s',
          this._subnode.id,
          peer,
          this._subnode.partition())
      }
      done(err)
    })
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

  leaveSelf () {
    this._onPeerLeft(this._subnode.id)
  }

  _onPeerJoined (peer) {
    debug('%s: peer %s joined', this._subnode.id, peer)
    console.log('%s: peer %s joined', this._subnode.id, peer)
    if (this._peers.indexOf(peer) < 0) {
      this._peers = this._peers.concat(peer)
    }
  }

  _onPeerLeft (peer) {
    debug('%s: peer %s left', this._subnode.id, peer)
    console.log('%s: peer %s left', this._subnode.id, peer)
    this._peers = this._peers.filter(p => p !== peer)
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
