'use strict'

class TimeoutError extends Error {
  constructor (peer, payload) {
    super(`operation timed out talking to ${peer}, payload is ${JSON.stringify(payload)}`)
    this.code = 'ETIMEOUT'
  }
}

module.exports = TimeoutError
