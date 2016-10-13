'use strict'

const debug = require('debug')('borough:subnode')
const async = require('async')
const EventEmitter = require('events')
const Skiff = require('skiff')
const Topology = require('./subnode-topology')

class Subnode extends EventEmitter {

  constructor (borough, address, partition, network, cluster, options) {
    debug('creating subnode with address %j for partition %s', address, partition)
    super()
    this.id = idFromAddress(address, partition)
    this._borough = borough
    this._address = address
    this._partition = partition
    this._network = network
    this._options = options
    this._startState = 'stopped'

    this._topology = new Topology(this, cluster, options)
    this._topology.on('warning', this.emit.bind(this, 'warning'))
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
    debug('creating skiff with peers %j', skiffOptions.peers)
    this._skiff = Skiff(idFromAddress(this._address, this._partition), skiffOptions)
    this._skiff.on('warning', this.emit.bind(this, 'warning'))
    this._skiff.on('new state', this._onNewState.bind(this))

    // skip sync new follower state
    async.series(
      [
        this._skiff.start.bind(this._skiff),
        done => {
          this._waitForState(options.waitForState, done)
          if (options.waitForState === 'weakened') {
            this._skiff.weaken(options.weakenDurationMS)
          }
        },
        done => this._topology.setPeers(this._options.peers, done)
      ], done)
  }

  _waitForState (states, done) {
    const skiff = this._skiff
    if (!Array.isArray(states)) {
      states = [states]
    }
    const currentState = skiff._node._stateName
    debug('%s: current state for %s is %s', this._skiff.id, skiff.id, currentState)
    if (states.find(s => skiff.is(s))) {
      process.nextTick(done)
    } else {
      skiff.on('new state', onStateChange)
    }

    function onStateChange (state) {
      if (states.indexOf(state) >= 0) {
        debug('_waitForState: %s reached state %s', skiff.id, state)
        skiff.removeListener('new state', onStateChange)
        done()
      }
    }
  }

  _onNewState (state) {
    debug('new state for node %s: %s', this._skiff.id, state)
    this._topology.topologyUpdated()
    this.emit('new state', state)
  }

  stop (done) {
    debug('stopping subode %s', this._skiff && this._skiff.id)
    if (this._skiff) {
      this._skiff.removeAllListeners()
      this._skiff.stop(done)
    } else {
      process.nextTick(done)
    }
  }

  // ------
  // Commands

  command (command, done) {
    debug('%s: command: %j', this._skiff.id, command)
    if (!this._skiff) {
      throw new Error('must start before')
    }
    this._skiff.command(command, done)
  }

  join (peer, done) {
    if (this.id !== peer) {
      this._skiff.join(peer, done)
    } else {
      // nevermind, trying to join self..
      process.nextTick(done)
    }
  }

  // ------
  // Topology

  topologyUpdated (force) {
    this._topology.topologyUpdated(force)
  }

  leaveSelf () {
    this._skiff.leave(this._subnode.id, this._warningOnError(() => {
      this._borough.leavePartition(this._partition)
    }))
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
