# Summary
- An example of a PersistentStreamProvider with a custom adapter.
- A way to implement replayable streams using a custom IQueueCache i.e. the client can ask to replay past events.
  - The cache can either be in-memory or on Redis. To use Redis for caching, set `UseRedisForCache="true"` in the provider line in server config.  
- You can configure Redis connection using:
    - `Server` => Redis server (default: "localhost:6379")  
    - `RedisDb` => database number (default: -1)

# Note
This is using an in-memory queue as a placeholder instead of a physical queue, for pratical use, you should replace that with your own queue choice.
The in-memory queue works fine when running this sample on the same machine with one server. However, this sample is writing from a grain i.e. the writes happen on the server and the reads also since the receiver is created on the server, so this setup works fine with an in-memory queue.

# Implementation
The stream is made replayable by having a IQueueCache implementation that keeps all the data that have been received so far.

# Sample
The sample provided will start a producer, wait a few seconds then start a consumer which will ask for the stream to replayed since the start. That last part is done by`await stream.SubscribeAsync(this, new SimpleSequenceToken(0));` i.e. by asking for event sequence token 0 which will always be the first message received on the stream.

The config is setup so that the server will use Redis for caching with the default connection parameters assuming Redis is running on the local machine.

# Future
- Make the physical queue pluggable i.e. make this usable with any given queue implementation. After that, remove `redis` branch and make it an option to use Redis as an example of a pluggable physical queue implementation.
- Clean up the memory cache to be simple and similar to the Redis one. It's currently very complicated because it was originally based on the implementation of one of the Azure streams.