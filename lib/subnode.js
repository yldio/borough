'use strict'

const debug = require('debug')('borough:subnode')
const async = require('async')
const EventEmitter = require('events')
const Skiff = require('skiff')
const IteratorStream = require('level-iterator-stream')
const Topology = require('./subnode-topology')

class Subnode extends EventEmitter {

  constructor (borough, address, partition, network, cluster, options) {
    debug('creating subnode with address %j for partition %s, with peers %j', address, partition, options.peers)
    super()
    this.id = idFromAddress(address, partition)
    this._borough = borough
    this._address = address
    this._partition = partition
    this._network = network
    this._cluster = cluster
    this._options = options
    this._startState = 'stopped'

    this._topology = new Topology(this, cluster, options)
    this._topology.on('warning', this.emit.bind(this, 'warning'))
    this._topology.start()
  }

  partition () {
    return this._partition
  }

  // ------
  // State

  is (state) {
    return this._skiff && this._skiff.is(state)
  }

  // ------
  // Start and stop

  start (options, done) {
    if (!done && (typeof options === 'function')) {
      done = options
      options = {}
    }
    debug('starting subnode for partition %s with options %j', this._partition, options)
    if (this._startState === 'stopped') {
      this._startState = 'starting'
      this._start(options, err => {
        if (err) {
          this._startState = 'stopped'
          done(err)
        } else {
          this._startState = 'started'
          this.emit('started')
          done()
        }
      })
    } else if (this._startState === 'started') {
      process.nextTick(done)
    } else if (this._startState === 'starting') {
      this.once('started', done)
    }

    if (options.forceRemotes) {
      this.topologyUpdated(true)
    }
  }

  _start (options, done) {
    const skiffOptions = Object.assign({}, this._options.skiff, {
      peers: this._options.peers,
      network: this._network
    })
    debug('%s: creating skiff with peers %j', this.id, skiffOptions.peers)

    this._skiff = Skiff(idFromAddress(this._address, this._partition), skiffOptions)
    this._skiff.on('warning', this.emit.bind(this, 'warning'))
    this._skiff.on('new state', this._onNewState.bind(this))
    this._skiff.on('joined', this._peerJoined.bind(this))
    this._skiff.on('left', this._peerLeft.bind(this))

    // skip sync new follower state
    async.series(
      [
        this._skiff.start.bind(this._skiff),
        done => {
          this._waitForState(options.waitForState, done)
          if (options.waitForState === 'weakened') {
            this._skiff.weaken(options.weakenDurationMS)
          }
        }
      ], done)
  }

  _waitForState (states, done) {
    const self = this
    const skiff = this._skiff
    if (!Array.isArray(states)) {
      states = [states]
    }
    const currentState = skiff._node._stateName
    debug('%s: current state for %s is %s', this.id, this.id, currentState)
    if (states.find(s => skiff.is(s))) {
      process.nextTick(done)
    } else {
      skiff.on('new state', onStateChange)
    }

    function onStateChange (state) {
      if (states.indexOf(state) >= 0) {
        debug('%s: _waitForState: %s reached state %s', self.id, self.id, state)
        skiff.removeListener('new state', onStateChange)
        done()
      }
    }
  }

  _onNewState (state) {
    debug('new state for node %s: %s', this.id, state)
    if (state === 'candidate') {
      this._maybeWeaken()
    } else if (state === 'leader') {
      this._topology.topologyUpdated()
    }
    this.emit('new state', state)
  }

  _maybeWeaken () {
    // I'm a candidate
    // let's see if I'm the supposed leader
    debug('%s: maybe weaken?', this.id)
    const leader = this._cluster.leaderForPartition(this._partition)
    if (leader !== this._cluster.whoami()) {
      debug('%s: i should not be the leader', this.id)
      // I shouldn't be the leader. Let's see if the leader is up..
      this._isLeaderUp((err, isUp) => {
        if (err) {
          this.emit('warning', err)
        } else {
          debug('%s: leader is up', this.id)
          // Am I still a candidate and is the leader up?
          if (this.is('candidate') && isUp) {
            debug('%s: leader is up and it\'s not me: weakening myself', this.id)
            this._skiff.weaken(this._options.weakenWhenCandidateAndLeaderIsUpDurationMS)
          }
        }
      })
    }
  }

  _isLeaderUp (done) {
    this._cluster.ping(this._cluster.leaderForPartition(this._partition), (err) => {
      done(err, !err)
    })
  }

  stop (done) {
    debug('%s: stopping', this.id)
    this._topology.stop()
    if (this._skiff) {
      this._skiff.stop(done)
    } else {
      process.nextTick(done)
    }
  }

  // ------
  // Commands

  command (command, done) {
    if (command.type === 'batch') {
      command = command.array
    }
    debug('%s: command: %j', this.id, command)
    if (!this._skiff) {
      throw new Error('must start before')
    }
    this._skiff.command(command, done)
  }

  // ------
  // Topology

  topologyUpdated (force) {
    this._topology.topologyUpdated(force)
  }

  join (peer, done) {
    debug('%s: skiff.join %s', this.id, peer)
    this._skiff.join(peer, done)
  }

  leave (peer, done) {
    debug('%s: skiff.leave %s', this.id, peer)
    this._skiff.leave(peer, done)
  }

  _peerLeft (peer) {
    debug('%s: peer %s left', this.id, peer)
    this._topology.peerLeft(peer)
    if (this.id === peer) {
      this._leftSelf()
    }
  }

  _leftSelf () {
    this._borough.leavePartition(this._partition, this._warningOnError())
  }

  _peerJoined (peer) {
    debug('%s: peer %s joined', this.id, peer)
    this._topology.peerJoined(peer)
  }

  _warningOnError (cb) {
    return err => {
      if (err) {
        debug('warning:', err.message)
        this.emit('warning', err)
      }
      if (cb) {
        cb()
      }
    }
  }

  // ------
  // Info

  info (done) {
    this._skiff.peers((err, peers) => {
      if (err) {
        done(err)
      } else {
        done(null, {
          source: this.id,
          peers
        })
      }
    })
  }

  // -----
  // Read stream

  readStream (options) {
    debug('%s: creating read stream with options %j', this.id, options)
    return new IteratorStream(this._skiff.leveldown().iterator(options))
  }
}

Subnode.idFromAddress = idFromAddress

function idFromAddress (address, partition) {
  if ((typeof address.host !== 'string') || (typeof address.port !== 'number')) {
    throw new Error('invalid address: ' + JSON.stringify(address))
  }
  const addr = `/ip4/${address.host}/tcp/${address.port}/p/${partition}`
  return addr
}

module.exports = Subnode
