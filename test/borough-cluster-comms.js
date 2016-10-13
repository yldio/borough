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

describe('borough cluster comms', () => {
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

  after({timeout: 10000}, done => {
    async.each(nodes.concat(baseNode), (node, cb) => node.stop(cb), done)
  })

  it('can setup a request handler', done => {
    nodes.concat(baseNode).forEach(node => {
      node.on('request', onRequest)
    })
    done()

    function onRequest (req, reply) {
      expect(req.partition.name).to.equal('partition 1')
      reply(null, Object.assign({}, req.body, {reply: true}))
    }
  })

  it('can make a request from a random node', {timeout: 10000}, done => {
    const node = nodes[nodes.length - 2]
    node.request('partition 1', {a: 1, b: 2}, (err, result) => {
      expect(err).to.be.null()
      expect(result).to.equal({a: 1, b: 2, reply: true})
      done()
    })
  })
})
