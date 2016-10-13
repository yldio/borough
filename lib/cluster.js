'use strict'

const debug = require('debug')('borough:cluster')

const Upring = require('upring')
const EventEmitter = require('events')
const merge = require('deepmerge')

const Connections = require('./cluster-connections')

const interestingEvents = [
  'move', 'steal', 'peerUp', 'peerDowm'
]

const commands = [ 'put', 'get', 'del', 'join', 'leave' ]

class Cluster extends EventEmitter {

  constructor (borough, options) {
    super()
    this._borough = borough
    this._options = options
    this._started = false

    const upringOpts = merge(this._options.upring, {
      base: options.base,
      name: options.name
    })
    debug('creating hashring from options %j', upringOpts)
    this._hashring = Upring(upringOpts)
    this._hashring.once('up', () => {
      this._started = true
    })

    this._connections = new Connections(this._hashring, this._options)
    this._connections.on('error', err => this.emit('error', err))

    interestingEvents.forEach(event => this._hashring.on(event, this.emit.bind(this, event)))

    this._hashring.add('ping', this._onLocalPing.bind(this))
    this._hashring.add('user request', this._onLocalUserRequest.bind(this))
    this._hashring.add('ensure partition', this._onLocalEnsurePartitionRequest.bind(this))
    this._hashring.add('partition address', this._onLocalPartitionAddressRequest.bind(this))
    this._hashring.add('info', this._onLocalInfoRequest.bind(this))
    commands.forEach(
      command => this._hashring.add({ cmd: { type: command } },
      this._onLocalCommandRequest.bind(this)))
  }

  // ------
  // Start and stop

  start (done) {
    debug('starting..')
    if (this._started) {
      process.nextTick(done)
    } else {
      this._hashring.once('up', () => { done() })
    }
  }

  stop (done) {
    debug('stopping..')
    this._connections.stop()
    this._hashring.close(err => {
      debug('closed hashring', err)
      done(err)
    })
  }

  // ------
  // Topology

  whoami () {
    return this._hashring.whoami()
  }

  nodesForPartition (partition, excludeSelf) {
    debug('%s: getting nodes for partition:', this._skiff && this._skiff.id)
    const leader = this._hashring._hashring.lookup(partition)
    debug('%s: leader for partition %s is %j', this._skiff && this._skiff.id, partition, leader.id)
    let nodes = [leader.id].concat(this._nextNodes(partition, this._options.redundancy, [leader.id]))
    if (excludeSelf) {
      const self = this.whoami()
      nodes = nodes.filter(n => n !== self)
    }
    return nodes
  }

  _nextNodes (partition, count, _exclude) {
    let node
    const nodes = []
    const exclude = _exclude.slice()
    do {
      node = this._hashring._hashring.next(partition, exclude)
      if (node) {
        nodes.push(node.id)
        exclude.push(node.id)
      }
    } while (nodes.length < count && node)

    debug('next nodes for partition %s are: %j', partition, nodes)

    return nodes
  }

  ensureRemotePartition (partition, peer, done) {
    debug('adding remote partition %s to %s', partition, peer)
    this._connections.request(
      peer,
      {
        cmd: 'ensure partition',
        partition
      },
      { timeout: this._options.remotePartitionAddressTimeoutMS },
      done)
  }

  _onLocalEnsurePartitionRequest (req, reply) {
    this._borough.partitionSubnode(
      req.partition,
      false,
      (err, subnode) => {
        if (err) {
          reply(err)
        } else {
          reply(null, subnode.id)
        }
      }
    )
  }

  remotePartitionAddress (partition, peer, done) {
    debug('%s: getting remote partition address for peer %j and partition %j', this._borough.whoami(), peer, partition)
    if (peer === this._borough.whoami()) {
      debug('%s: getting address locally..', this._borough.whoami())
      done(null, this._borough.localPartitionSubnodeAddress(partition))
    } else {
      this._connections.request(
        peer,
        {
          cmd: 'partition address',
          partition
        },
        { timeout: this._options.remotePartitionAddressTimeoutMS },
        done)
    }
  }

  _onLocalPartitionAddressRequest (req, reply) {
    debug('%s: getting local partition address for partition %j', this._borough.whoami(), req.partition)
    reply(null, this._borough.localPartitionSubnodeAddress(req.partition))
  }

  ping (peer, done) {
    this._connections.request(peer, { cmd: 'ping' }, {}, done)
  }

  _onLocalPing (req, reply) {
    reply(null, { ok: true })
  }

  _onLocalInfoRequest (req, reply) {
    this._borough.localPartitionInfo(req.key, reply)
  }

  // ------
  // Commands

  command (partition, command, done) {
    debug('command (partition: %s, command: %j)', partition, command)
    this._hashring.request(
      {
        key: partition,
        cmd: command
      },
      done)
  }

  _onLocalCommandRequest (req, reply) {
    debug('local request (command: %j)', req.cmd)
    this._borough.localCommand(req.key, req.cmd, reply)
  }

  userRequest (partition, req, done) {
    debug('user request (partition = %j, req = %j)', partition, req)
    this._hashring.request(
      {
        key: partition,
        cmd: 'user request',
        req
      },
      done)
  }

  _onLocalUserRequest (req, reply) {
    debug('local user request %j', req)
    this._borough.localUserRequest(req.key, req.req, reply)
  }
}

module.exports = Cluster
