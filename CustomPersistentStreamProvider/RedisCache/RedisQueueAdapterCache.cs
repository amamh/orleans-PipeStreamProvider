using Orleans.Runtime;
using Orleans.Streams;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PipeStreamProvider.RedisCache
{
    public class QueueAdapterCacheRedis : IQueueAdapterCache
    {
        private readonly Logger _logger;
        private readonly IDatabase _db;
        private readonly ConcurrentDictionary<QueueId, QueueCacheRedis> _caches;

        public int Size
        {
            get
            {
                var total = 0;
                foreach (var cache in _caches)
                    total += cache.Value.Size;
                return total;
            }
        }

        public QueueAdapterCacheRedis(Logger logger, IDatabase db)
        {
            _logger = logger;
            _db = db;
            _caches = new ConcurrentDictionary<QueueId, QueueCacheRedis>();
        }

        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            return _caches.AddOrUpdate(queueId, (id) => new QueueCacheRedis(id, _logger, _db), (id, queueCache) => queueCache);
        }
    }
}
