# Channel API Examples

Although some examples are given in-line with the specification, they are generally focused on a specific aspect being discussed, and are not complete examples of how you might use channels in a real application.

This document fleshes out those examples, adding more code and commentary to each, and includes new examples focused on scenarios not necessary to explain the spec. It is meant to be standalone, so an astute reader will notice some text duplication with the specification.

## Channels

### Overview

Channels provide a mechanism to transport data from one part of the application to another. In a nutshell channels are just a pipes that have two endpoints, one for putting data onto them and other for taking data out of them:

```js
const { input, output } = new Channel()

input.put(1)
output.take()
```

Channels provide certain guarantees to allow reasosing about the data flow and to allow coordinatation of the data flow with in the application:

1. Channels act like queues and operate on [FIFO][] basis.
1. Any `put` on a channel blocks the task (not an execution, but rather semantics) until put is complete. Blocking is expressed via returned promise that is resolved on completion of the put operation.
1. Data can be `put` on a channel even when it is blocked, in which case operation is enqueued.
1. Any `take` on a channel blocks task until data is available (is put) on a channel.
1. When more data is taken of the channel than present take operations are enqueued.
1. Channels can be closed from either (`input` / `output`) end of it.
1. Data put on a closed channel is dropped and results in promise that is resolved with `undefined`.
1. If `undefined` is put on a channel it's just dropped (In a way `undefined` properties are dropped by JSON).
1. When channel is closed all the pending takes are resolved with `undefined`.
1. Data put on a channel can still be taken after it is closed.
1. Taking data of the closed channel that does not contains it results in promise resolved to `undefined` (In a way reading deleted property results in `undefined`).


### Usage

#### Basics


#### Pumping a Channel input To the Console

Although the by-far most common way of consuming a channel is to transform it from one form to another, it is useful to see some examples to understand how the underlying primitives work. Consider following `print` function that writes the contents of a given channel to the console:

```js
function print(input) {
  // Function that is going to process data chunk once
  // data is available and task is remused.
  function next(chunk) {
    // If chunk is `undefined` then channel is closed
    // and no more data is left.
    if (chunk === void(0)) {
      console.log("--- all done!")
    }
    // Otherwise log the `chunk` and take a next one.
    else {
      console.log(chunk)
      input.take().then(next)
    }
  }
  input.take().then(next)
}

var channel = new Channel()
print(channel.input)

var output = channel.output
output.put(1)
output.put(2)
output.put(3)
output.close()
```

#### Pumping array content intou Channel output

Although sending array content one element at a time isn't practical it still a useful example for explaning channels. Consider following example:

```js
function putAll(output, chunks) {
  return new Promise(function(resolve, reject) {
    var index = 0
    function pump(result) {
      if (index < chunks.length) {
        output.put(chunks[index]).then(pump)
        index = index + 1
      } else {
        resolve(result)
      }
    }

    pump()
  })
}

var channel = new Channel()
putAll(channel.output, [1, 2, 3, 4])
print(channel.input)
```

Note that in the above example `putAll` fully respects speed at which channel input is consumed and next data chunk is put when previous one is complete. It is typically a good idea to obay the backpressure, but still useful to have a choice. We could have ignored backpressure and implemented same function as follows:

```js
function putAll(output, chunks) {
 var result = void(0)
 var index = 0
 while (index < chunks.length) {
   result = output.put(chunks[index])
   index = index + 1
 }

 return result
}
```

Both of the implementations would behave the same although later one will cause channel to queue `put` operations and complete them when possible. Also note that result will be a promise that is resolved once last `put` is complete so in that regard behavior is the same. While in case of arrays there is no practical difference weather backpressure is respected or not there are planty of cases where it does.


#### Coordination

We have being talking about blocking channels for quite some time but the truth no execution has being blocked anywhere (that's definitely for the best :). Blocking term has being used in a conceptual sense and implemented via promises that resolve when task is unblocked. This allows coorditanion between producer and a consumer of the data that is transported via channels (note that backpressure is on form of such coordination).

Unfortunately this coordination and blocking has being hidden behind promises and their handler continuations (callbacks). With a little bit of sugar and modern features of JS language (like generators) everything can be expressed in more natural way, also this conceptual blocking can become more apparent.


Let's consider same `print` function but implemented using little bit of sugar:

```js
var print = function print(input) {
  // Spawn an async task that is blocked when promise is yield and
  // resumed when promise is resolved.
  spawn(function*() {
    var chunk = void(0)
    //  Block generator until `chunk` is received on
    // `input`. If `chunk` is `undefined` then all
    // chunks were already received and channel is closed,
    // otherwise log received chunk.
    while (chunk = yield input.take(), chunk !== void(0))
      console.log(chunk)

    console.log("--- all done!")
  })
}
```

In this example `input.take()` does actually block the execution of the spawned task (but nothing else) until the corresponding data is present on the channel.

This covers the consumption part but now let's consider the producer part and implement our first `putAll` example:

```js
function putAll(output, chunks) {
  return spawn(function*() {
    var index = 0
    var result = void(0)
    while (index < chunks.length) {
      result = yield output.put(chunks[index])
      index = index + 1
    }
    return result
  })
}
```

Did you notice how similar this is to the example of `putAll` that was not respecting a blocking, even though in this case it does ? In fact version that does not respect backpressure is almost identical:

```js
function putAll(output, chunks) {
  return spawn(function*() {
    var index = 0
    var result = void(0)
    while (index < chunks.length) {
      result = output.put(chunks[index])
      index = index + 1
    }
    return result
  })
}
```

Only difference is in absence of `yield` keyword  which makes up a pretty simple rule, if you would like to coordinate with other end of the channel just put `yield` in front of `put` or `take`.


Here is an example of how above used `spawn` function can be implemented although note that it is expected to get a natural successor to it in ES7 in form of `async await` syntax additions:

```js
function spawn(routine) {
  return new Promise(function(resolve, reject) {
    var task = routine();
    var raise = function(error) {
      task.throw(error)
    }
    var next = function(data) {
      var step = task.next(data)
      if (step.done)
        resolve(step.value)
      else
        Promise.cast(step.value).then(next, raise)
    }
    next()
  })
}
```

#### Buffered channels

So far all of the examples have being either coordinating producer and consumer of the channel or have being completely ignoring each other. Both are very practical and let you decide weather you want coordination between different parts of application or not. The problem is though if you choose to synchronize producer and a consumer they would have to operate with a same speed essentially forcing one of them oparate slower and wasting CPU cycles (instead of fetching data over the network for example). If you decide against synchronization than you can wind up in a situation where lot of memory is being wasted on queued up take or put operations. Not to worry though not all is that bad, all one needs is to find a right balance between two in order to compensate speed difference between producer and consumer. This is typically achieved by bufferring, at the instatiation channel can be given a buffer that is used by a channel to accumulate data in it. Until buffer is full puts will not block, after it is full they will block and be queued as it is a case with unbuffered channels.

When channel is given an number as an argument at the instatiation it will have fixed size buffer of that number:

```js
var channel = new Channel(3)
var output = channel.output

output.put(1) // => Promise <true>
output.put(2) // => Promise <true>
output.put(3) // => Promise <true>
output.put(4) // => Promise:pending
```

In the example above first three items `1, 2, 3` are going to be bufferred and there for not block (return pre-resolved promise) while last one will block until `channel.input.take()` occurs. This allows producer and consumer to have their own schedules and don't waste time on waiting each other.


As a matter of fact `new Channel(n)` is just a sugar for creating a channel with fixed size buffer of `3` items:

```js
var buffer = new FixedBuffer(3)
var channel = new Channel(buffer)
```

This not only allows you to have some leeway between producer and consumer schedules but it also completely separates concerns of transporting data from the data itself. What this means that depending on use case different buffering strategies can be used without having to change anything about transporting layer (channels).

For example consider implementing sliding buffer that will drop oldests data chunk bufferred when buffer is full:


```js
function SlidingBuffer(size) {
  this.buffer = []
  this.size = size
}
SlidingBuffer.prototype.isEmpty = function() {
  return this.buffer.length === 0
}
// Sliding buffer is never full since it just drops oldest
// chunks of data when overflowing.
SlidingBuffer.prototype.isFull = function() {
  return false
}
SlidingBuffer.prototype.take = function() {
  return this.buffer.pop()
}
SlidingBuffer.prototype.put = function(data) {
  if (this.buffer.length === this.size)
    this.take()

  this.buffer.unshift(data)
}

var channel = new Channel(new SlidingBuffer(30))
```

above channel will never block as it's buffer is never going to be full. It is also easy to implement other types of buffers maybe something like `DroppingBuffer` that would drop items once overflowing. Or consider a case when streaming video where equivalent keyframes can be dropped if buffer is about to overflow.


#### Maximizing throughput

Now if you have being following closely you may be thinking that channels impose a delay at which data can be consumed given that coordination happens through promises. That is only partially true, and should not be a problem in practice, give that all the I/O takes place off the main thread data can be pumped into per tick anyhow.

Although argument can be made what if the channel has being laying arround for some time accumulating data before it was ever consumed. Well first off all yielding back to event loop is typically a not a bad thing, but putting that aside it is always possible to take more the one chunk at a time to process all of them with in one tick:

```js
var firstThree = [input.take(), input.take(), input.take()]
Promise.all(firstThree).then(function([first, second, third]) {
})
```

Taking multiple chunks at a time to process works well when consumer knows how much data it needs to do it's task. But sometimes it may make more sense to say take as much as it is present on a channel. It is important to realize that this is not transporation (channel) problem but rather a storage (bufferring) problem, since we would like to store data on a channel such that it all can be consumed at once. Given that we identified problem we can implement a simple solution for it:

```js
function AggregateBuffer(size) {
  this.size = size
  this.chunks = []
}
AggregateBuffer.prototype.isEmpty = function() {
  return this.chunks.length === 0
}
AggregateBuffer.prototype.isFull = function() {
  return this.chunks.length === this.size
}
AggregateBuffer.prototype.take = function() {
  return this.chunks.splice(0)
}
AggregateBuffer.prototype.put = function(chunk) {
  this.chunks.push(chunk)
}

var aggregate = new AggregateBuffer(30)
var channel = new Channel(aggregate)
var output = channel.output
var input = channel.input

output.put(1) // => Promise <true>
output.put(2) // => Promise <true>
output.put(3) // => Promise <true>

input.take()  // => Promise <[1, 2, 3]>
```

When dealing with I/O it is likely that aggregate style buffer will come handy so let's define one such buffer that is not only responsible for aggregating data but also knows type of the data & there for aggregates it in a way that makes most sense to consumer:

```js
function ByteBuffer(size) {
  this.size = size
  this.chunks = []
  this.byteLength = 0
}
ByteBuffer.prototype.isEmpty = function() {
  return this.byteLength === 0
}
ByteBuffer.prototype.isFull = function() {
  return this.size <= this.byteLength
}
ByteBuffer.prototype.put = function(chunk) {
  if (!(chunk instanceof ArrayBuffer))
    throw TypeError("Can only put ArrayBuffer")
  this.chunks.push(chunk)
  this.byteLength = this.byteLength + chunk.byteLength
}
ByteBuffer.prototype.take = function() {
  var result = new ArrayBuffer(this.byteLength)
  var chunks = this.chunks.splice(0)

  this.byteLength = 0
  var index = 0
  var offset = 0
  while (index < chunks.length) {
    var chunk = chunks[index]
    var element = 0
    while (element < chunk.byteLength) {
      result[offset] = chunk[element]
      element = element + 1
      offset = offset + 1
    }
    index = index + 1
  }

  return result
}


var buffer = new ByteBuffer(1024)
var channel = new Channel(buffer)

channel.output.put(new TextEncoder("utf-8").encode("hello").buffer) // => Promise <true>
channel.output.put(new TextEncoder("utf-8").encode(" ").buffer)     // => Promise <true>
channel.output.put(new TextEncoder("utf-8").encode("world").buffer) // => Promise <true>

channel.input.take().then(function(x) {
  return TextDecoder("utf-8").decode(new Uint8Array(x))
}) // => Promise <"hello world">
```


### Push or Pull

In general whether to use push or pull tends to be a subject of big debate. Producers tend to prefer push based approach while consumers tend to prefer pull based approach. Truth is the only way to satisfy both producer and a consumer is to put a queue between them and let them make choice of whether to use pull or push independently of each other based of their own domain. As a matter of fact buffered channels do exactly that they are just a data queues with internal buffer that is used to smooth out I/O between producer and consumer. Note that when buffer is full, puts are not lost, but rather queued up applying backpressure on the producer. Choosing right buffer for the task is crucial to arranging best use of resources.


#### Adapting a Push-Based Data Source

In general, a push-based data source can be modeled as:

- A `resume` method that starts the flow of data.
- A `pause` method that pauses the flow of the data.
- A `close` method that sends an advisory signal to stop the flow of data
- A `ondata` handler that fires when new data is pushed from the source
- A `onend` handler that fires when the source has no more data
- A `onerror` handler that fires when the source signals an error getting data.

As an aside, this is pretty close to the existing HTML [`WebSocket` interface](http://www.whatwg.org/specs/web-apps/current-work/multipage/network.html#the-websocket-interface), with the exception that `WebSocket` does not give any method of pausing or resuming the flow of data.

Let's assume we have some raw C++ socket object or similar, which presents the above API. The data it delivers via `ondata` comes in the form of `ArrayBuffer`s. We wish to create a class that wraps that C++ interface into a stream, with a configurable high-water mark set to a reasonable default. This is how you would do it:

```js
var $input = "@@socket/input";
var $error = "@@socket/error";
function Socket(host, port, opts) {
  opts = opts || {};
  var highWaterMark = opts.highWaterMark || 16 * 1024;
  // Use earlier defined ByteBuffer that will take care of
  // bufferring data and blocking puts if it's full (over the
  // `highWaterMark`).
  var buffer = new ByteBuffer(highWaterMark);
  var input = this[$input] = new Channel(buffer);
  var error = this[$error] = new Channel();

  var rawSocket = createRawSocket(host, port);


  function onerror(e) {
    error.output.put(e);
  }

  function onend() {
    data.output.close();
  }

  function onput() {
    // Resume only when buffer is empty again.
    if (buffer.isEmpty()) {
      rawSocket.resume();
    }
  }

  function ondata(chunk) {
    data.output.put(chunk).then(onput);
    // Pause whenever buffer is full.
    if (buffer.isFull()) {
      rawSocket.pause();
    }
  }


  rawSocket.onerror = onerror;
  rawSocket.onend = onend;
  rawSocket.ondata = ondata;

  rawSocket.resume();
}
StreamingSocket.prototype = {
  constructor: StreamingSocket,
  get input() { return this[$input].input },
  get error() { return this[$error].input }
}

var client = new Socket("http://example.com", 80);
print(client.input);
```


By leveraging channels API it's really simple to keep track of the bufferred data, which can be used to apply backpressure strategy on the underlaying raw socket. If allocated buffer size fills up to the high water mark (defaulting to 16 KiB), signal is sent to the underlying socket that it should stop sending us data. And once the consumer drains all the queued data, it will send the start signal back, resuming the flow of data.

#### Adapting a Pull-Based Data Source

In general, a pull-based data source can be modeled as:

- An `open(cb)` method that gains access to the source; it can call `cb` either synchronous or asynchronously, with either `(err)` or `(null)`.
- A `read(cb)` function method that gets data from the source; can call `cb` either synchronously or asynchronously, with either `(err, null, null)` indicating an error, or `(null, true, null)` indicating there is no more data, or `(null, false, data)` indicating there is data.
- A `close(cb)` method that releases access to the source; can call `cb` either synchronously or asynchronously, with either `(err)` or `(null)`.

Let's assume that we have some raw C++ file handle API matching this type of setup. Here is how we would adapt that into a readable stream:


```js
function open(filename) {
  return new Promise(function(resolve, reject) {
    var handle = createRawFileHandle(filename);
    handle.open(function(error) {
      if (error) reject(error);
      else resolve(handle);
    });
  });
}

function read(handle) {
  return new Promise(function(resolve, reject) {
    handle.read(function(error, done, data) {
      if (error) reject(error);
      else if (done) resolve(void(0));
      else resolve(data);
    });
  });
}

function close(handle) {
  return new Promise(function (resolve, reject) {
    handle.close(function (error) {
      if (error) reject(error);
      else resolve(void(0));
    });
  });
}


function readFile(path, opts) {
  opts = opts || {};
  var highWaterMark = opts.highWaterMark || 16 * 1024;
  var output = new Channel(highWaterMark);

  function* read() {
    var file = yield open(filename);
    var chunk = void(0);

    try {
      while (chunk = yield read(file), chunk !== void(0)) {
        yield output.output.put(chunk);
      }
    } finally {
      yield close(file);
    }
  }

  spawn(read);
  return output;
}
```



[Task.js]:http://taskjs.org/
[FIFO]:http://en.wikipedia.org/wiki/FIFO
