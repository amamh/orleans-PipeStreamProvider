using Orleans;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PipeStreamProvider.RedisCache
{
    public class QueueCacheRedis : IQueueCache
    {
        private readonly ConnectionMultiplexer _conn;
        private readonly Logger _logger;
        private string _redisHashName;
        private IDatabase _db;
        private readonly RedisCustomList<IBatchContainer> _cache;

        public QueueId Id { get; }
        public int MaxAddCount { get; }

        public int Size => _cache.Count;

        public QueueCacheRedis(QueueId id, Logger logger, IDatabase db)
        {
            Id = id;
            _db = db;
            _logger = logger;
            // FIXME: What else do we need to make sure no clash happens?
            _redisHashName = $"orleans-pipecache-{id}";
            _cache = new RedisCustomList<IBatchContainer>(_db, _redisHashName, _logger);
        }

        public void AddToCache(IList<IBatchContainer> messages)
        {
            foreach (var item in messages)
                if (!_cache.RightPush(item))
                    _logger.AutoError($"Couldn't add batch to cache. Namespace: {item.StreamNamespace}, Stream: {item.StreamGuid}, Queue Cache ID: {Id}");
        }

        public IQueueCacheCursor GetCacheCursor(Guid streamGuid, string streamNamespace, StreamSequenceToken token)
        {
            return new QueueCacheRedisCursor(_cache, streamNamespace, streamGuid, token);
        }

        public bool IsUnderPressure()
        {
            // FIXME
            return false;
        }

        public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
        {
            // FIXME
            purgedItems = null;

            return true;
        }
    }
}