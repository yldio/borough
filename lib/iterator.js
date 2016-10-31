'use strict'

const debug = require('debug')('borough:iterator')
const AbstractIterator = require('abstract-leveldown').AbstractIterator
const through = require('through2')

class Iterator extends AbstractIterator {

  constructor (db, cluster, partition, options) {
    debug('%s: creating iterator for partition %s', cluster.whoami(), partition)
    super(db)
    this._stream = through.obj()

    const req = {
      type: 'read stream',
      options
    }

    debug('sending cluster command %j', req)
    cluster.command(partition, req, (err, result) => {
      debug('cluster command replied', err, result)
      if (err) {
        this._stream.emit('error', err)
      } else {
        result.streams.read.pipe(this._stream)
      }
    })
  }

  _next (callback, cleanup) {
    debug('_next')
    if (cleanup) {
      cleanup()
    }
    this._tryRead(callback)
  }

  _tryRead (callback) {
    debug('tryRead')
    // const callback = once(callback)
    const self = this
    let onReadable
    const item = this._stream.read()

    if (item) {
      debug('have item: %j', item)
      callback(null, item.key, item.value)
    } else {
      onReadable = this._next.bind(this, callback, cleanup)
      debug('waiting for readable..')
      this._stream.on('readable', onReadable)
      this._stream.on('end', callbackAndCleanup)
      this._stream.on('error', callbackAndCleanup)
    }

    function cleanup () {
      self._stream.removeListener('readable', onReadable)
      self._stream.removeListener('end', cleanup)
      self._stream.removeListener('error', cleanup)
    }

    function callbackAndCleanup (err) {
      cleanup()
      callback(err)
    }
  }

  _end (callback) {
    if (this._stream) {
      this._stream.end()
    } else {
      process.nextTick(callback)
    }
  }
}

module.exports = Iterator
