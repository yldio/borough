'use strict'

const lab = exports.lab = require('lab').script()
const describe = lab.experiment
const before = lab.before
const after = lab.after
const it = lab.it
const expect = require('code').expect

const Memdown = require('memdown')
const timers = require('timers')
const async = require('async')

const Borough = require('../')

const CLIENT_TIMEOUT_MS = 10000 // TODO: take this down

describe('borough cluster topology changes', () => {
  let working = true
  let baseNode
  let nodes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
  let peerCount
  let counter = 0

  function onRequest (req, reply) {
    const part = req.partition
    expect(part.name).to.equal('partition 1')
    const body = req.body
    if (body.type === 'put') {
      part.put(body.key, body.value, reply)
    } else if (body.type === 'get') {
      part.get(body.key, reply)
    } else {
      reply(new Error('unknown op ' + body.type))
    }
  }

  before(done => {
    baseNode = Borough({
      subnode: {
        skiff: {
          db: Memdown
        }
      }
    })
    baseNode.on('request', onRequest)
    baseNode.on('warning', logError)
    baseNode.start(done)
  })

  before(done => {
    let lastValue

    const partition = baseNode.partition('partition 1')

    timers.setInterval(() => {
      partition.info((err, info) => {
        if (err) {
          console.error('Error getting info:', err.message)
        } else {
          const peers = info.subnode.peers
          console.log('\n%d peers. peers: %j', peers.length, peers)
        }
      })
    }, 1000)

    request()
    done()

    function request () {
      if (!working) return
      const timeout = timers.setTimeout(onTimeout, CLIENT_TIMEOUT_MS)
      const isPut = !(counter % 2)
      const isGet = !isPut
      if (isPut) {
        lastValue = counter
      }
      counter++

      if (isGet) {
        partition.get('a', (err, resp) => {
          timers.clearTimeout(timeout)
          if (err) {
            return handleError(err)
          }
          process.stdout.write('.')
          expect(err).to.be.null()
          expect(resp).to.equal(lastValue)
          process.nextTick(request)
        })
      } else {
        partition.put('a', lastValue, err => {
          timers.clearTimeout(timeout)
          if (err) {
            return handleError(err)
          }
          process.stdout.write('.')
          process.nextTick(request)
        })
      }

      function onTimeout () {
        console.error('REQUEST TIMEOUT')
        if (working) {
          throw new Error('request timeout')
        }
      }

      function handleError (err) {
        if (working) {
          throw err
        }
      }
    }
  })

  after(done => {
    working = false
    async.parallel(
      [
        baseNode.stop.bind(baseNode),
        done => {
          async.each(nodes, (node, done) => {
            if ((typeof node) === 'object') {
              console.log('stopping %s', node.whoami())
              node.stop((err) => {
                console.log('stopped %s', node.whoami(), err)
                done(err)
              })
            } else {
              done()
            }
          }, done)
        }
      ],
      done)
  })

  it('can rail in nodes', {timeout: (nodes.length * 2) * 11000}, done => {
    async.eachSeries(
      nodes,
      (index, done) => {
        timers.setTimeout(() => {
          console.log('NEW NODE %d\n\n\n\n\n', index)
          const newNode = nodes[index] = Borough({
            base: [baseNode.whoami()],
            subnode: {
              skiff: {
                db: Memdown
              }
            }
          })
          newNode.on('request', onRequest)
          newNode.on('warning', logError)
          newNode.start(done)
        }, 8000)
      },
      done)
  })

  it('waits a bit', {timeout: 6000}, done => timers.setTimeout(done, 5000))

  it('partition has only 3 nodes', done => {
    baseNode.partition('partition 1').info((err, info) => {
      expect(!err).to.be.true()
      expect(info.subnode.source).to.match(/^\/ip4\/.*\/p\/partition 1$/)
      peerCount = info.subnode.peers.length
      expect(peerCount).to.equal(3)
      done()
    })
  })

  it('a big amount of requests were performed', done => {
    const minimum = 500 * nodes.length
    expect(counter).to.be.least(minimum)
    done()
  })

  it('rails out nodes', {timeout: (nodes.length * 2) * 11000}, done => {
    let count = nodes.length + 1
    async.eachSeries(
      nodes.slice(1),
      (node, done) => {
        timers.setTimeout(() => {
          count--
          console.log('\n\nstopping node %d...\n\n\n', count)
          node.stop(err => {
            console.log('\n\nstopped.')
            if (err) {
              done(err)
            } else {
              nodes = nodes.filter(n => n !== node)
              done()
            }
          })
        }, 12000)
      },
      done)
  })

  it('waits a bit', {timeout: CLIENT_TIMEOUT_MS + 1000}, done => timers.setTimeout(done, CLIENT_TIMEOUT_MS + 500))

  it('partition has only 2 nodes', done => {
    baseNode.partition('partition 1').info((err, info) => {
      expect(!err).to.be.true()
      peerCount = info.subnode.peers.length
      expect(peerCount).to.equal(2)
      done()
    })
  })
})

function logError (err) {
  console.error(err.stack)
}
