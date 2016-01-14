[![Build status](https://ci.appveyor.com/api/projects/status/ip5irt07j6hr7b6v?svg=true)](https://ci.appveyor.com/project/amamh/orleans-pipestreamprovider)

# Summary
- An example of a PersistentStreamProvider with a custom adapter.
- Uses **Redis** lists as queues.
- A way to implement **replayable streams** using a custom IQueueCache i.e. the client can ask to replay past events.
- A token has a timestamp. A client can request to replay since time X by passing a token with a timestamp in the past.

# Physical Queue
Currently, the main physical queue implmentation is using Redis and is used by default. You can also use an in-memory queue, but that's a bad idea.
The in-memory queue will only work when writing from within the server (i.e. from a grain).

# Config
- `UseRedisForQueue="true"` to use Redis for the physical queue (default: "true")
- Redis connection:
    - `Server` => Redis server (default: "localhost:6379")  
    - `RedisDb` => database number (default: -1)

# Sample
Starts two clients, one writes to a stream and the other subscribes to it.
You need to run Redis first.

# Future
- Make the physical queues pluggable as external libraries.