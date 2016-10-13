'use strict'

const lab = exports.lab = require('lab').script()
const describe = lab.experiment
const before = lab.before
const after = lab.after
const it = lab.it
const expect = require('code').expect

const async = require('async')
const Memdown = require('memdown')

const Borough = require('../')

describe('borough cluster partition data', () => {
  let baseNode
  let nodes = [1, 2, 3, 4]

  before(done => {
    baseNode = Borough({
      subnode: {
        skiff: {
          db: Memdown
        }
      }
    })
    baseNode.start(done)
  })

  before(done => {
    nodes = nodes.map(index => Borough({
      base: [baseNode.whoami()],
      subnode: {
        skiff: {
          db: Memdown
        }
      }
    }))
    done()
  })

  before({timeout: 10000}, done => {
    async.each(nodes, (node, cb) => node.start(cb), done)
  })

  after(done => {
    async.each(nodes.concat(baseNode), (node, cb) => node.stop(cb), done)
  })

  it('can setup a request handler', done => {
    nodes.concat(baseNode).forEach(node => {
      node.on('request', onRequest)
    })
    done()

    function onRequest (req, reply) {
      const body = req.body
      const part = body.otherPartition ? req.otherPartition(body.otherPartition) : req.partition
      if (body.type === 'put') {
        part.put(body.key, body.value, reply)
      } else if (body.type === 'get') {
        part.get(body.key, reply)
      } else {
        reply(new Error('command type not found'))
      }
    }
  })

  it('can make a put request on partition 1 from a random node', {timeout: 10000}, done => {
    const node = nodes[nodes.length - 2]
    node.request(
      'partition 1',
      {
        type: 'put',
        key: 'a',
        value: 'b',
        otherPartition: 'partition 2'
      },
      done)
  })

  it('can make a put request on partition 2 from a random node', done => {
    const node = nodes[nodes.length - 2]
    node.request(
      'partition 2',
      {
        type: 'put',
        key: 'c',
        value: 'd',
        otherPartition: 'partition 1'
      }, done)
  })

  it('data got stored in the correct partition', done => {
    const node = nodes[nodes.length - 1]
    node.request('partition 1', {type: 'get', key: 'c'}, (err, result) => {
      expect(err).to.be.null()
      expect(result).to.equal('d')
      done()
    })
  })

  it('data did not get stored in the wrong partition', done => {
    const node = nodes[nodes.length - 1]
    node.request('partition 2', {type: 'get', key: 'c'}, (err, result) => {
      expect(err.message).to.match(/key not found in database/i)
      done()
    })
  })
})
