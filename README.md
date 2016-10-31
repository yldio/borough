![Borough](borough.png)

    Partitioned, fault-tolerant and survivable database and application server.

The marriage of a Hashring, the Raft consensus protocol and an embedded LevelDB database.

Ensures that all the data your application needs is local. Automatic redundancy and fail-over.

# Install and use

```bash
$ npm install borough --save
```

```javascript
const Borough = require('borough')

const node = Borough()

node.on('request', handleRequest)

function handleRequest (req, reply) {
  console.log('handling partition %s', req,partition.name)

  // use local partition
  req.partition.get('key', (err, value) => {
      // ...
  })

  // use remote partition
  req.partition('partition name').put('key', {some: 'value'}, err => {
    // ...
  })
}
```

# API:

# Borough (options)

Creates and returns a Borough node.

```javascript
const Borough = require('borough')
const options = {
  // ...
}
const node = Borough(options)
```

Options:

* base (array, defaults to []): The other peers that constitute the initial cluster.
* TODO: rest

# node

## node.start (callback)

Start the node. Invokes callback when successful or when an error occurs, with an error as the first argument.

## node.partition (partition)

Returns a partition instance, which is a [LevelDown](https://github.com/level/leveldown#readme) instance.

## node.request (partition, payload, callback)

Make a request to the cluster. Arguments:

* `partition` (string): the partition name to make the request to
* `payload` (object): the request object, can be any object. All the streams contained in the `streams` attribute will be streamed.
* `callback` (function (err, reply)): a function that gets called when there is a reply. If there was an error, first argument contains it. If not, second argument contains the reply object. All the streams contained in the `reply.streams` attribute will be streamed in.

## node.join (address, cb)

Make the instance join another node or set of nodes. The callback is called after the join is completed, or with error as first argument if not.

## node.whoami ()

Returns the identifier string for this node.

## Events emitted by node

* `request (request, reply)`: when there is a request for a partition.
  * `request`: an instance of the `Request` class (see below).
  * `reply` (function): A function you have to call to reply. Accepts any object as payload. All the streams contained in the `streams` attribute will be streamed in.

# Request

## request.partition

The current partition

## request.body

The request body. All the streams contained in the `request.body.streams` attribute will be streamed in.

## request.otherPartition(partitionName)

Access a different, (probably remote) partition. Returns a partition object.

# Partition

The partition exposed as [a LevelDown-compatible interface](https://github.com/level/leveldown#readme).

## partition.name

Contains the name of the partition.

## partition.get(key, callback)

Reads a partition value from a key.

## partition.put(key, value, callback)

Writes a partition value into the specified key.

## partition.iterator(options)

Gets an iterator

## partition.info(callback)

Gets some partition statistics. Arguments:

* `callback` (function, mandatory): gets called once there is info about the partition:

```js
partition.info((err, info) => {
  if (err) {
    console.error(err)
  } else {
    console.log('info: %j', info)
  }
})
```

`info` is an object contaning:

* `peers` (array): a list of the skiff peer addresses for that partition

# Sponsors

Borough development is sponsored by [YLD](https://www.yld.io).

