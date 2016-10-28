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

describe('borough partition leveldown interface', () => {
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
    baseNode.on('request', onRequest)
    baseNode.start(done)
  })

  before(done => {
    nodes = nodes.map((index) => Borough({
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

  before(done => {
    nodes.forEach(node => {
      node.on('request', onRequest)
    })
    done()
  })

  after(done => {
    async.each(nodes.concat(baseNode), (node, cb) => node.stop(cb), done)
  })

  it('can make a put request from a random node', done => {
    const node = nodes[nodes.length - 4]
    node.request('partition 1', {type: 'put', key: 'a', value: 'b'}, done)
  })

  it('can make a get request from a random node', done => {
    const node = nodes[nodes.length - 3]
    node.request('partition 1', {type: 'get', key: 'a'}, (err, result) => {
      expect(err).to.be.null()
      expect(result).to.equal('b')
      done()
    })
  })

  it('can make a del request from a random node', done => {
    const node = nodes[nodes.length - 2]
    node.request('partition 1', {type: 'del', key: 'a' }, (err, result) => {
      expect(!err).to.be.true()
      done()
    })
  })

  it('can make a get request from a random node', done => {
    const node = nodes[nodes.length - 1]
    node.request('partition 1', {type: 'get', key: 'a'}, (err, result) => {
      expect(!!err).to.be.true()
      expect(err.message).to.equal('Key not found in database')
      done()
    })
  })

  it('can make a batch request from a random node', done => {
    const node = nodes[nodes.length - 4]
    const array = [
      { type: 'put', key: 'a', value: 'c' },
      { type: 'put', key: 'b', value: 'd' },
      { type: 'put', key: 'c', value: 'e' }
    ]
    node.request('partition 1', {type: 'batch', array }, (err, result) => {
      expect(!err).to.be.true()
      done()
    })
  })

  it('can make a get request from a random node', done => {
    const node = nodes[nodes.length - 3]
    node.request('partition 1', {type: 'get', key: 'a'}, (err, result) => {
      expect(!err).to.be.true()
      expect(result).to.equal('c')
      done()
    })
  })

  it('can make a get request from a random node', done => {
    const node = nodes[nodes.length - 2]
    node.request('partition 1', {type: 'get', key: 'c'}, (err, result) => {
      expect(!err).to.be.true()
      expect(result).to.equal('e')
      done()
    })
  })
})

function onRequest (req, reply) {
  expect(req.partition.name).to.equal('partition 1')
  const body = req.body
  if (body.type === 'put') {
    req.partition.put(body.key, body.value, reply)
  } else if (body.type === 'get') {
    req.partition.get(body.key, reply)
  } else if (body.type === 'del') {
    req.partition.del(body.key, reply)
  } else if (body.type === 'batch') {
    req.partition.batch(body.array, reply)
  } else {
    reply(new Error('command type not found'))
  }
}
