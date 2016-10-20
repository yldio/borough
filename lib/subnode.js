'use strict'

const debug = require('debug')('borough:subnode')
const async = require('async')
const EventEmitter = require('events')
const Skiff = require('skiff')
const Topology = require('./subnode-topology')
const timers = require('timers')

class Subnode extends EventEmitter {

  constructor (borough, address, partition, network, cluster, options) {
    debug('creating subnode with address %j for partition %s', address, partition)
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
    this._skiff.on('joined', peer => this._topology.peerJoined(peer))
    this._skiff.on('left', peer => this._topology.peerLeft(peer))

    console.log('  ----> %s: created skiff with peers %j', this._skiff.id, this._options.peers)

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
    debug('new state for node %s: %s', this._skiff.id, state)
    console.log('new state for node %s: %s', this._skiff.id, state)
    if (state === 'candidate') {
      this._maybeWeaken()
    }
    this._topology.topologyUpdated()
    this.emit('new state', state)
  }

  _maybeWeaken () {
    // I'm a candidate
    // let's see if I'm the supposed leader
    console.log('maybe weaken?')
    const leader = this._cluster.leaderForPartition(this._partition)
    if (leader !== this._cluster.whoami()) {
      console.log('i should not be the leader')
      // I shouldn't be the leader. Let's see if the leader is up..
      this._isLeaderUp((err, isUp) => {
        if (err) {
          this.emit('warning', err)
        } else {
          console.log('leader is up')
          // Am I still a candidate and is the leader up?
          if (this.is('candidate') && isUp) {
            console.log('%s: leader is up and it\'s not me: weakening myself', this.id)
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

  leaveSelf (done) {
    debug('%s: leaveSelf', this.id)
    console.log('%s: skiff.leave()', this.id)
    this._skiff.leave(this.id, err => {
      debug('%s: left self', this.id, err)
      if (err) {
        done(err)
      } else {
        console.log('%s: leaving partition..', this.id)
        this._borough.leavePartition(this._partition, done)
      }
    })
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
