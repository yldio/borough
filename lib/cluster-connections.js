'use strict'

const debug = require('debug')('borough:cluster.connections')
const timers = require('timers')
const EventEmitter = require('events')
const once = require('once')
const merge = require('deepmerge')

const TimeoutError = require('./timeout-error')

class ClusterConnections extends EventEmitter {

  constructor (hashring, options) {
    super()
    this._hashring = hashring
    this._options = options
  }

  request (peerId, payload, _options, _done) {
    if (!peerId) {
      throw new Error('need peer id')
    }

    if ((typeof peerId) !== 'string') {
      throw new Error('peer id should be string')
    }
    debug('requesting %s, payload = %j', peerId, payload)

    const done = once(_done)

    const options = merge(
      {
        timeout: this._options.requestTimeoutMS,
        tries: 1
      },
      _options || {})

    if (options.tries > this._options.maxRetries) {
      return done(new Error('exceeded retry count'))
    }

    let peer
    try {
      peer = this._hashring._hashring.peers(true).find(p => p.id === peerId)
    } catch (err) {
      if (err.message.match(/hashring not up yet/i)) {
        debug('local hashring not up yet, retrying once it\'s up')
        this._hashring.once('up', () => {
          debug('hashring is up, going to retry request..')
          this.request(peerId, payload, options, done)
        })
      } else {
        done(err)
      }
      return
    }

    if (peer) {
      const conn = this._hashring.peerConn(peer)

      const timeout = timers.setTimeout(() => {
        debug('request timed out after %d ms', this._options.requestTimeoutMS)
        done(new TimeoutError(peerId, payload))
      }, options.timeout)

      debug('have peer connection, doing the request %j now', payload)
      conn.request(payload, (err, result) => {
        debug('request replied err = %j, result = %j', err && err.message, result)
        timers.clearTimeout(timeout)
        if (err && err.message.match(/hashring not up yet/i)) {
          timers.setTimeout(() => {
            options.tries ++
            this.request(peerId, payload, options, done)
          }, this._options.retryOnWarningMS)
        } else {
          done(err, result)
        }
      })
    } else {
      debug('peer %s not found', peerId)
      done(new Error(`peer ${peerId} not found`))
    }
  }

  stop () {
    // nothing to do here..
  }
}

module.exports = ClusterConnections
