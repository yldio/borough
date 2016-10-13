'use strict'

const networkAddress = require('network-address')

module.exports = {
  base: [],
  address: {
    host: networkAddress()
  },
  cluster: {
    redundancy: 2,
    name: 'borough',
    upring: {
      logLevel: 'error',
      hashring: {}
    },
    requestTimeoutMS: 5000,
    retryPersistentRequestMS: 4000,
    retryOnWarningMS: 500,
    maximumRetries: 10,
    remotePartitionAddressTimeoutMS: 10000
  },
  subnode: {
    skiff: {
      rpcTimeoutMS: 30000
    },
    quitter: {
      pollTimeoutMS: 5000
    },
    peers: []
  },
  secondarySubnodeWeakenAtStartupMS: 10000,
  debounceTopologyChangesMS: 4000
}
