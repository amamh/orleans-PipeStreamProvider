using System;
using System.Collections.Generic;
using System.Diagnostics;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace PipeStreamProvider.Cache
{
    public class QueueCache : IQueueCache
    {
        private readonly Logger _logger;
        private readonly LinkedList<PipeQueueAdapterBatchContainer> _cache;
        private readonly LinkedList<QueueCacheCursor> _cursors;
        private readonly TimeSpan _timeUntilObsolete;

        public QueueId Id { get; }
        public int MaxAddCount { get; } = 1024; // some sensible number

        public int Size => (int)_cache.Count; // WARNING

        public QueueCache(QueueId id, TimeSpan timeUntilObsolete, Logger logger)
        {
            Id = id;
            _timeUntilObsolete = timeUntilObsolete;
            _logger = logger;
            _cache = new LinkedList<PipeQueueAdapterBatchContainer>();
            _cursors = new LinkedList<QueueCacheCursor>();
        }

        public void AddToCache(IList<IBatchContainer> messages)
        {
            // We do the purging here so that it triggers before each additing
            PurgeOld();

            foreach (var item in messages)
            {
                Debug.Assert(item is PipeQueueAdapterBatchContainer);
                _cache.AddLast(item as PipeQueueAdapterBatchContainer);
            }
        }

        private int PurgeOld()
        {
            var timeNow = DateTime.UtcNow;
            var numPurged = 0;
            while ((timeNow - _cache.First?.Value?.RealToken.Timestamp) > _timeUntilObsolete)
            {
                _cache.RemoveFirst();
                numPurged++;
            }
            // advance delayed cursors:
            var earliestBatch = _cache.First;


            if (earliestBatch == null)
            {
                Debug.Assert(_cache.Count == 0);
                // Unset all cursors
                foreach (var cursor in _cursors)
                    cursor.IsSet = false;
            }
            else {
                foreach (var cursor in _cursors)
                    if (cursor.CurrentToken.Older(earliestBatch.Value.RealToken))
                        cursor.IsSet = false;
            }


            // TODO: Log
            return numPurged;
        }

        public IQueueCacheCursor GetCacheCursor(Guid streamGuid, string streamNamespace, StreamSequenceToken token)
        {
            if (token != null && !(token is TimeSequenceToken))
            {
                // Null token can come from a stream subscriber that is just interested to start consuming from latest (the most recent event added to the cache).
                throw new ArgumentOutOfRangeException(nameof(token), "token must be of type TimeSequenceToken");
            }

            var cursor = new QueueCacheCursor(_cache, streamNamespace, streamGuid, (TimeSequenceToken)token, _logger);
            _cursors.AddLast(cursor);
            return cursor;
        }

        public bool IsUnderPressure()
        {
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