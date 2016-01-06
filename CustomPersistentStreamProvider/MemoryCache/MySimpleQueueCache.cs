using System;
using System.Collections.Generic;
using System.Diagnostics;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

// TODO: Scrap this and do it from scratch without all this complexity
namespace PipeStreamProvider.MemoryCache
{
    public class QueueCache : IQueueCache
    {
        private readonly Logger _logger;
        private readonly LinkedList<IBatchContainer> _cache;

        public QueueId Id { get; }
        public int MaxAddCount { get; } = 1024; // some sensible number

        public int Size => (int)_cache.Count; // WARNING

        public QueueCache(QueueId id, Logger logger)
        {
            Id = id;
            _logger = logger;
            _cache = new LinkedList<IBatchContainer>();
        }

        public void AddToCache(IList<IBatchContainer> messages)
        {
            foreach (var item in messages)
                _cache.AddLast(item);
        }

        public IQueueCacheCursor GetCacheCursor(Guid streamGuid, string streamNamespace, StreamSequenceToken token)
        {
            if (token != null && !(token is SimpleSequenceToken))
            {
                // Null token can come from a stream subscriber that is just interested to start consuming from latest (the most recent event added to the cache).
                throw new ArgumentOutOfRangeException(nameof(token), "token must be of type SimpleSequenceToken");
            }

            return new QueueCacheCursor(_cache, streamNamespace, streamGuid, (SimpleSequenceToken)token, _logger);
        }

        public bool IsUnderPressure()
        {
            // TODO: How to detect if Redis is under pressure?
            return false;
        }

        public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
        {
            // we don't purge
            purgedItems = null;
            return true;
        }
    }
}