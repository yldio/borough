'use strict'

class TimeoutError extends Error {
  constructor () {
    super('operation timed out')
    this.code = 'ETIMEOUT'
  }
}

module.exports = TimeoutError
