using System;
using System.Collections.Concurrent;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace PipeStreamProvider.MemoryCache
{
    class MySimpleQueueAdapterCache : IQueueAdapterCache
    {
        private readonly Logger _logger;
        private readonly ConcurrentDictionary<QueueId, IQueueCache> _caches;


        public MySimpleQueueAdapterCache(IQueueAdapterFactory factory, Logger logger)
        {
            this._logger = logger;
            _caches = new ConcurrentDictionary<QueueId, IQueueCache>();
        }

        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            return _caches.AddOrUpdate(queueId, (id) => new QueueCache(id, _logger), (id, queueCache) => queueCache);
        }

        public int Size
        {
            get { return _caches.Select(pair => pair.Value.Size).Sum(); }
        }
    }
}