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

// TODO: We can improve the performance using pipelining: _db.CreateTransaction
// This will at least reduce communication. In Python, a speedup of 5x can be achieved http://blog.jmoz.co.uk/python-redis-py-pipeline/
// Will probably have a bigger impact if Redis is running on a different machine.

namespace PipeStreamProvider.RedisCache
{
    /// <summary>
    /// A custom data structure with O(1) append/prepend and access. Does not support insertion.
    /// Internally this is using a Redis hash where keys are indices.
    /// </summary>
    public class RedisCustomList<T> : ICacheList<T>
    {
        private readonly IDatabase _db;
        private readonly RedisKey _key;
        private readonly Logger _logger;
        private long _lastIndx = 0;
        private long _firstIndx = 0;
        public long Count { get; private set; } = 0;

        public RedisCustomList(IDatabase db, string redisKey, Logger logger)
        {
            _db = db;
            _logger = logger;
            _key = redisKey;

            if (_db.KeyExistsAsync(redisKey).Result)
            {
                _logger.AutoWarn($"Redis hashset already exists with name {redisKey}. Deleting it.");
                _db.KeyDeleteAsync(redisKey).Wait();
            }
        }

        public T Get(long index)
        {
            AssertExists(index);

            var bytes = _db.HashGet(_key, index);
            var item = SerializationManager.DeserializeFromByteArray<T>(bytes);
            return item;
        }

        public bool Set(long index, T newVal)
        {
            AssertExists(index);

            var bytes = SerializationManager.SerializeToByteArray(newVal);
            return _db.HashSet(_key, index, bytes);
        }

        public bool LeftPush(T v)
        {
            var serialised = SerializationManager.SerializeToByteArray(v);

            if (_db.HashSet(_key, _firstIndx - 1, serialised))
            {
                _firstIndx--;
                Count++;
                Debug.Assert(_db.HashLength(_key) == Count);
                return true;
            }
            return false;
        }

        public bool RightPush(T v)
        {
            var serialised = SerializationManager.SerializeToByteArray(v);

            if (_db.HashSet(_key, _lastIndx + 1, serialised))
            {
                _lastIndx++;
                Count++;
                Debug.Assert(_db.HashLength(_key) == Count);
                return true;
            }
            return false;
        }

        public T LeftPop()
        {
            if (Count == 0)
                throw new Exception("Cannot pop list with no elements");

            var bytes = _db.HashGet(_key, _firstIndx);
            var item = SerializationManager.DeserializeFromByteArray<T>(bytes);
            _db.HashDelete(_key, _firstIndx);

            Count--;
            Debug.Assert(_db.HashLength(_key) == Count);
            _firstIndx = _firstIndx == 0 ? 0 : _firstIndx++;

            return item;
        }

        public T RightPop()
        {
            if (Count == 0)
                throw new Exception("Cannot pop list with no elements");

            var bytes = _db.HashGet(_key, _lastIndx);
            var item = SerializationManager.DeserializeFromByteArray<T>(bytes);
            _db.HashDelete(_key, _lastIndx);

            Count--;
            Debug.Assert(_db.HashLength(_key) == Count);
            _lastIndx = _lastIndx == 0 ? 0 : _lastIndx--;

            return item;
        }

        private void AssertExists(long index)
        {
            if (index < _firstIndx || index > _lastIndx || Count == 0)
                throw new QueueCacheMissException($"Trying to get index {index} from redis linked list which is outside range {_firstIndx}-{_lastIndx}.");

            Debug.Assert(_db.KeyExists(_key) && _db.HashExists(_key, index));
        }
    }
}
