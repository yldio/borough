'use strict'

const lab = exports.lab = require('lab').script()
const describe = lab.experiment
const before = lab.before
const it = lab.it
const expect = require('code').expect

const Memdown = require('memdown')
const timers = require('timers')

const Borough = require('../')

describe('borough one node cluster', () => {
  let node

  it('allows you to create a node with no options', done => {
    node = Borough({subnode: { skiff: { db: Memdown } }})
    done()
  })

  it('can start the node', done => node.start(done))

  before({timeout: 5000}, done => timers.setTimeout(done, 4000))

  it('can setup a request handler', done => {
    node.on('request', (req, reply) => {
      expect(req.partition.name).to.equal('partition 1')
      expect(req.body).to.equal({a: 1, b: 2})
      reply(null, Object.assign({}, req.body, {replied: true}))
    })
    done()
  })

  it('can perform a request', done => {
    node.request('partition 1', {a: 1, b: 2}, (err, reply) => {
      expect(err).to.be.null()
      expect(reply).to.equal({a: 1, b: 2, replied: true})
      done()
    })
  })
})
