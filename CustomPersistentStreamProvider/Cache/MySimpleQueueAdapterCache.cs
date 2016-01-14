using System;
using System.Collections.Concurrent;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace PipeStreamProvider.Cache
{
    class MySimpleQueueAdapterCache : IQueueAdapterCache
    {
        private readonly Logger _logger;
        private readonly ConcurrentDictionary<QueueId, IQueueCache> _caches;
        private readonly TimeSpan _timeToKeepMessages;

        public MySimpleQueueAdapterCache(IQueueAdapterFactory factory, TimeSpan timeToKeepMessages, Logger logger)
        {
            _timeToKeepMessages = timeToKeepMessages;
            _logger = logger;
            _caches = new ConcurrentDictionary<QueueId, IQueueCache>();
        }

        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            return _caches.AddOrUpdate(queueId, (id) => new QueueCache(id, _timeToKeepMessages, _logger), (id, queueCache) => queueCache);
        }

        public int Size
        {
            get { return _caches.Select(pair => pair.Value.Size).Sum(); }
        }
    }
}