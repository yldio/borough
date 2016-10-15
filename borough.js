'use strict'

const debug = require('debug')('borough.borough')
const EventEmitter = require('events')
const async = require('async')
const merge = require('deepmerge')
const Skiff = require('skiff')
const freeport = require('freeport')
const clone = require('clone-deep')

const Cluster = require('./lib/cluster')
const Subnode = require('./lib/subnode')
const Request = require('./lib/request')
const Partition = require('./lib/partition')
const defaultOptions = require('./lib/default-options')

class Borough extends EventEmitter {

  constructor (options) {
    super()
    debug('new node with options %j', options)
    this._options = merge(clone(defaultOptions), options || {})
    this._startState = 'stopped'

    this._address = clone(this._options.address)
    this._partitions = {}
  }

  // ----
  // Start and stop

  start (done) {
    debug('starting borough node...')
    if (this._startState !== 'stopped') {
      throw new Error('already starting...')
    }
    this._startState = 'starting'
    async.series(
      [
        this._startNetwork.bind(this),
        this._startCluster.bind(this)
      ],
      err => {
        this._startState = 'started'
        debug('started: %j', err)
        done(err)
      })
  }

  stop (done) {
    debug('stopping borough node...')
    this._startState = 'stopped'
    async.series(
      [
        this._stopNetwork.bind(this),
        this._stopAllSubnodes.bind(this),
        this._stopCluster.bind(this)
      ],
      done)
  }

  _startCluster (done) {
    const options = merge(this._options.cluster, {
      base: this._options.base,
      address: this._address
    })
    this._cluster = new Cluster(this, options)

    this._cluster.start(done)
    this._cluster.on('error', err => this.emit('error', err))
    this._cluster.on('peerUp', this._onTopologyChange.bind(this))
    this._cluster.on('peerDown', this._onTopologyChange.bind(this))
  }

  _startNetwork (done) {
    freeport((err, port) => {
      if (err) {
        return done(err)
      }
      this._address.port = port

      this._network = Skiff.createNetwork({
        passive: {
          server: clone(this._address)
        }
      })
      this._network.active.on('error', (err) => {
        this.emit('error', err)
      })
      this._network.passive.on('error', (err) => {
        this.emit('error', err)
      })
      process.nextTick(done)
    })
  }

  _stopNetwork (done) {
    if (this._network) {
      debug('stopping network..')
      this._network.passive.once('closed', err => {
        debug('network stopped', err)
        done(err)
      })
      this._network.active.end()
      this._network.passive.end()
    } else {
      process.nextTick(done)
    }
  }

  _stopAllSubnodes (done) {
    const partitions = Object.keys(this._partitions)
    debug('stopping all %d subnodes..', partitions.length)
    async.each(
      partitions.map(part => this._partitions[part]),
      (subnode, cb) => subnode.then(
          sn => sn.stop(cb),
          err => {
            debug('error stopping node:', err.message)
            this.emit('warning', err)
            cb()
          }),
      err => {
        debug('all subnodes stopped', err)
        done(err)
      })
  }

  _stopCluster (done) {
    debug('stopping cluster..')
    this._cluster.stop(err => {
      debug('cluster stopped', err)
      done(err)
    })
  }

  // ----
  // Topology

  whoami () {
    return this._cluster.whoami()
  }

  _onTopologyChange () {
    debug('%s: topology changed', this.whoami())
    const partitions = Object.keys(this._partitions)
    partitions.forEach(this._reconfigurePartition.bind(this))
  }

  _reconfigurePartition (partition) {
    debug('%s: reconfiguring partition %s', this.whoami(), partition)
    this.partitionSubnode(partition, false, (err, subnode) => {
      if (err) {
        this.emit('warning', err)
      } else {
        subnode.topologyUpdated()
      }
    })
  }

  _partitionSubnodeAddresses (partition, done) {
    const nodeAddresses = this._cluster.nodesForPartition(partition)
    debug('%s: node addresses for partition %s: %j', this.whoami(), partition, nodeAddresses)
    async.map(
      nodeAddresses,
      this._cluster.remotePartitionAddress.bind(this._cluster, partition),
      (err, addresses) => {
        debug('%s: subnode addresses result: err = %j, addresses = %j', err && err.message, addresses)
        if (!err && addresses) {
          done(null, addresses.filter(a => !!a))
        } else {
          done(err)
        }
      })
  }

  partitionSubnode (partition, forceRemotes, done) {
    debug('node for partition %j, force remotes = %j', partition, forceRemotes)
    const subnode = this._partitions[partition]
    if (!subnode) {
      debug('does not exist yet, creating partition subnode for partition %s', partition)
      this._createPartitionSubnode(partition, forceRemotes, err => {
        if (err) {
          debug('error creating partition subnode:', err)
          this.emit('warning', err)
        } else {
          this._partitions[partition].then(
            sn => done(null, sn),
            err => done(err))
        }
      })
    } else {
      subnode.then(
        sn => done(null, sn),
        err => done(err))
    }
  }

  localPartitionSubnodeAddress (partition) {
    return this._address &&
      this._address.host &&
      this._address.port &&
      Subnode.idFromAddress(this._address, partition)
  }

  localPartitionInfo (partition, done) {
    this.partitionSubnode(partition, false, (err, subnode) => {
      if (err) {
        done(err)
      } else {
        subnode.info(done)
      }
    })
  }

  _createPartitionSubnode (partition, forceRemotes, done) {
    debug('%s: create partition subnode: %s', this.whoami(), partition)

    this._partitions[partition] = new Promise((resolve, reject) => {
      debug('%s: getting partition subnode addresses..', this.whoami())
      this._partitionSubnodeAddresses(partition, (err, peers) => {
        debug('%s: partition subnode addresses result: err = %j, peers = %j', this.whoami(), err && err.message, peers)
        if (err) {
          reject(err)
          done(err)
        } else {
          debug('%s: peer subnode addresses for partition %s: %j', this.whoami(), partition, peers)
          const options = merge(this._options.subnode, { peers })
          const subnode = new Subnode(
            this,
            this._address,
            partition,
            this._network,
            this._cluster,
            options)

          subnode.on('warning', this.emit.bind(this, 'warning'))

          debug('%s: starting node for partition %s...', this.whoami(), partition)

          const shouldBeLeader = (peers.indexOf(subnode.id) === 0)
          if (shouldBeLeader) {
            debug('%s: I should be leader of partition %s', this.whoami(), partition)
          }
          const startOptions = {
            waitForState: shouldBeLeader ? 'leader' : 'weakened',
            weakenDurationMS: this._options.secondarySubnodeWeakenAtStartupMS,
            forceRemotes
          }

          subnode.start(startOptions, err => {
            debug('%s: subnode for partition %s started', this.whoami(), partition)
            if (err) {
              reject(err)
              done(err)
            } else {
              resolve(subnode)
              done(null, subnode)
            }
          })
        }
      })
    })
  }

  leavePartition (partition, done) {
    const node = this._partitions[partition]
    if (node) {
      delete this._partitions[partition]
      node.then(node => {
        node.removeAllListeners()
        node.stop(done)
      },
      done)
    } else {
      process.nextTick(done)
    }
  }

  // ----
  // Operations

  request (partition, req, done) {
    this._cluster.userRequest(partition, req, done)
  }

  remoteCommand (partition, command, done) {
    this._cluster.command(partition, command, done)
  }

  localCommand (partition, command, done) {
    debug('%s: local command (partition = %j, command = %j)', this.whoami(), partition, command)
    this.partitionSubnode(partition, true, (err, subnode) => {
      if (err) {
        done(err)
      } else {
        subnode.command(command, done)
      }
    })
  }

  localUserRequest (partition, req, reply) {
    debug('%s: local user request (partition = %j, req = %j)', this.whoami(), partition, req)
    this.partitionSubnode(partition, true, (err, subnode) => {
      if (err) {
        reply(err)
      } else {
        const haveListeners = this.emit(
          'request',
          new Request(this.partition(partition), req, this),
          reply)
        if (!haveListeners) {
          reply(new Error('no request listener'))
        }
      }
    })
  }

  partition (partition) {
    return new Partition(partition, this)
  }
}

module.exports = createBorough

function createBorough (options) {
  return new Borough(options)
}
