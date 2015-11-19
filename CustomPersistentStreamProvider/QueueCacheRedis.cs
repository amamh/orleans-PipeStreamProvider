using Orleans.Runtime;
using Orleans.Streams;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PipeStreamProvider
{

    class MyCacheItem {
        public string StreamNamespace { get; set; }
        public Guid StreamGuid { get; set; }
        public IBatchContainer Container { get; set; }
    }

    class RedisFastLinkedList {
        private readonly IDatabase _db;
        private readonly RedisKey _key;
        private readonly Logger _logger;
        private int _index = 0;

        public RedisFastLinkedList(IDatabase db, string redisKey, Logger logger) {
            _db = db;
            _logger = logger;
            _key = redisKey;

            if (_db.KeyExistsAsync(redisKey).Result)
            {
                _logger.AutoWarn($"Redis hashset already exists with name {redisKey}. Deleting it.");
                _db.KeyDeleteAsync(redisKey).Wait();
            }
        }

        public bool Append(string streamNamespace, Guid stream,  IBatchContainer v)
        {
            var item = new MyCacheItem
            {
                StreamNamespace = streamNamespace,
                StreamGuid = stream,
                Container = v,
            };

            RedisKey field = _index.ToString();
            var serialised = item.ToString(); //FIXME


            return _db.HashSet(_key, _index, serialised);
        }
    }

    public class QueueCacheRedis : IQueueCache
    {
        private readonly ConnectionMultiplexer _conn;
        private readonly Logger _logger;
        private string _redisHashName;
        private IDatabase _db;
        private readonly RedisFastLinkedList _cache;

        public QueueId Id { get; }
        public int MaxAddCount { get; }

        public int Size { get; }

        public QueueCacheRedis(QueueId id, Logger logger, IDatabase db)
        {
            Id = id;
            _db = db;
            _logger = logger;
            // FIXME: What else do we need to make sure no clash happens?
            _redisHashName = $"orleans-pipecache-{id}";
            _cache = new RedisFastLinkedList(_db, _redisHashName, _logger);
        }

        public void AddToCache(IList<IBatchContainer> messages)
        {
            foreach (var item in messages)
                if (!_cache.Append(item.StreamNamespace, item.StreamGuid, item))
                    _logger.AutoError($"Couldn't add batch to cache. Namespace: {item.StreamNamespace}, Stream: {item.StreamGuid}, Queue Cache ID: {Id}");
        }

        public IQueueCacheCursor GetCacheCursor(Guid streamGuid, string streamNamespace, StreamSequenceToken token)
        {
            throw new NotImplementedException();
        }

        public bool IsUnderPressure()
        {
            throw new NotImplementedException();
        }

        public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
        {
            throw new NotImplementedException();
        }
    }
}
