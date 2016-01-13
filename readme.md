[![Build status](https://ci.appveyor.com/api/projects/status/ip5irt07j6hr7b6v?svg=true)](https://ci.appveyor.com/project/amamh/orleans-pipestreamprovider)

# Summary
- An example of a PersistentStreamProvider with a custom adapter.
- A way to implement **replayable streams** using a custom IQueueCache i.e. the client can ask to replay past events.
  - The cache can either be in-memory or on Redis. To use Redis for caching, set `UseRedisForCache="true"` in the provider line in server config.  

# Physical Queue
Currently, the main physical queue implmentation is using Redis. You can also use an in-memory queue, but that's a bad idea.
The in-memory queue will only work when writing from within the server (i.e. from a grain). So, if you want to run the Sample
using the in-memory queue, you have to change the producer so that it sends the data to a grain which then writes to the stream
instead of writing to the stream directly.

Basically, enable Redis as the physical queue (already enabled in the config).

# Config
- Cache:
	- `UseRedisForCache="true"` to use Redis for caching.
	- `UseRedisForQueue="true"` to use Redis for the physical queue.

- Redis connection:
    - `Server` => Redis server (default: "localhost:6379")  
    - `RedisDb` => database number (default: -1)

# Sample
The sample provided will start a producer, wait a few seconds then start a consumer which will ask for the stream to replayed since the start. That last part is done by`await stream.SubscribeAsync(this, new SimpleSequenceToken(0));` i.e. by asking for event sequence token 0 which will always be the first message received on the stream.

The config is setup so that the server will use Redis for caching and as a physical queue.
It uses the default connection parameters assuming Redis is running on the local machine.

# Future
- Separate Redis connection parameters in the config for the cache and the physical queue. They shouldn't be assumed to be using the same Redis server.
- Make the physical queues pluggable as external libraries.