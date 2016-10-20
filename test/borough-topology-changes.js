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
  let nodes = [0, 1, 2, 3, 4, 5, 6, 7, 8]
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
    baseNode.start(done)
  })

  before(done => {
    let lastValue

    const partition = baseNode.partition('partition 1')

    timers.setInterval(() => {
      partition.info((err, info) => {
        if (err) {
          throw err
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
        handleError(new Error(`client timeout after ${counter} requests`))
      }

      function handleError (err) {
        console.error(err.stack)
        partition.info((err, info) => {
          if (info) {
            console.log('info:\n', info)
          }
          throw err
        })
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
              node.stop(done)
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
          newNode.start(done)
        }, 10000)
      },
      done)
  })

  it('waits a bit', {timeout: 12000}, done => timers.setTimeout(done, 11000))

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
    const minimum = 1000 * nodes.length
    expect(counter).to.be.least(minimum)
    done()
  })

  return;

  it('rails out nodes', {timeout: (nodes.length * 2) * 11000}, done => {
    async.eachSeries(
      nodes.slice(1), // all but the first node
      (node, done) => {
        timers.setTimeout(() => {
          baseNode.partition('partition 1').info((err, info) => {
            if (err) {
              throw rr
            } else {
              console.log('partition info:\n', info)
            }
          })
          console.log('stopping node...\n\n\n')
          node.stop(err => {
            if (err) {
              done(err)
            } else {
              nodes = nodes.filter(n => n !== node)
              done()
            }
          })
        }, 10000)
      },
      done)
  })

  it('waits a bit', {timeout: CLIENT_TIMEOUT_MS + 1000}, done => timers.setTimeout(done, CLIENT_TIMEOUT_MS + 500))
})
