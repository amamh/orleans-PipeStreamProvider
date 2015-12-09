# Summary
- An example of a PersistentStreamProvider with a custom adapter.
- A way to implement **replayable streams** using a custom IQueueCache i.e. the client can ask to replay past events.
  - The cache can either be in-memory or on Redis. To use Redis for caching, set `UseRedisForCache="true"` in the provider line in server config.  

# Physical Queue
You can use either Redis as a physical queue (a Redis list internally) or an in-memory queue. The in-memory queue is only good for debugging but it's better not to use it.
The in-memory queue works fine when running this sample on the same machine with one server. However, this sample is writing from a grain i.e. the writes happen on the server and the reads also since the receiver is created on the server, so this setup works fine with an in-memory queue.
It's recommended to use Redis as a physical queue, to do that put `UseRedisForQueue="true"` in the server config (it should be there by default).

# Config
- Cache:
	- `UseRedisForCache="true"` to use Redis for caching.
	- `UseRedisForQueue="true"` to use Redis for the physical queue.

- Redis connection:
    - `Server` => Redis server (default: "localhost:6379")  
    - `RedisDb` => database number (default: -1)

# Sample
The sample provided will start a producer, wait a few seconds then start a consumer which will ask for the stream to replayed since the start. That last part is done by`await stream.SubscribeAsync(this, new SimpleSequenceToken(0));` i.e. by asking for event sequence token 0 which will always be the first message received on the stream.

The config is setup so that the server will use Redis for caching with the default connection parameters assuming Redis is running on the local machine.

# Future
- Make the physical queues pluggable as external libraries.
- Clean up the memory cache to be simple and similar to the Redis one. It's currently very complicated because it was originally based on the implementation of one of the Azure streams.