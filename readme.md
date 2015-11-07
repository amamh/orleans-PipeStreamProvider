# Purpose

- An example of a PersistentStreamProvider with a custom adapter.
- A way to implement replayable streams using a custom IQueueCache i.e. the client can ask to replay past events.

# Implementation

This is based on the Azure Queue Stream Provider which is included in Orleans. The main change was replacing Azure Queue with a .NET Queue.


The stream is made replayable by having a new IQueueCache implementation instead of SimpleQueueCache which is used in Azure Queue Stream. The idea was to keep the original SimpleQueueCache as intact as possible to make the implementation simpler:

- Add a field `_coldCache`. This will hold all messages that get out of the normal/"hot" cache and will be used to replay.
- Change the three methods:
  - `EmptyBucket`:
    - Add to `_coldCache` every time a bucket is emptied i.e. `_coldCache` will have old messages.
  - `InitializeCursor`:
    - Check if the client wants older messages i.e. check the start SequenceToken and set the cache cursor accordingly.
  - `TryGetNextMessage`:
    - Do the same as before except we now have to account for the case when `_coldCache` has been replayed and the cursor now needs to point to the normal cache as is the case when the client doesn't ask to replay old messages.
  
# Sample

The sample provided will start a producer, wait a few seconds then start a consumer which will ask for the stream to replayed since the start. That last part is done by`await stream.SubscribeAsync(this, new EventSequenceToken(0));` i.e. by asking for event sequence token 0 which will always be the first message received on the stream.