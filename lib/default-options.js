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
    retryOnWarningMS: 500,
    remotePartitionAddressTimeoutMS: 10000,
    remotePartitionAddressRetryMS: 500,
    remotePartitionAddressMaxRetries: 10,
    maxRetries: 10,
    retryWaitMS: 200
  },
  subnode: {
    skiff: {
      rpcTimeoutMS: 30000
    },
    quitter: {
      pollTimeoutMS: 6000,
      delayQuittingMS: 6000
    },
    peers: [],
    weakenWhenCandidateAndLeaderIsUpDurationMS: 1000
  },
  secondarySubnodeWeakenAtStartupMS: 2000
}
