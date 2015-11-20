using System;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using StackExchange.Redis;
using PipeStreamProvider.RedisCache;

namespace PipeStreamProvider
{
    public class PipeQueueAdapterFactory : IQueueAdapterFactory
    {
        private const string CacheSizeParam = "CacheSize";
        private const int DefaultCacheSize = 4096;

        private const string NumQueuesParam = "NumQueues";
        private const int DefaultNumQueues = 8; // keep as power of 2.

        private const string ServerParam = "Server";
        private const string DefaultServer = "localhost:6379";

        private const string RedisDbParam = "RedisDb";
        private const int DefaultRedisDb = -1;

        private string _providerName;
        private Logger _logger;
        private int _cacheSize;
        private int _numQueues;
        private string _server;
        private HashRingBasedStreamQueueMapper _streamQueueMapper;
        private IQueueAdapterCache _adapterCache;
        private ConnectionMultiplexer _connection;
        private int _databaseNum;
        private IDatabase _db;

        public void Init(IProviderConfiguration config, string providerName, Logger logger)
        {
            _logger = logger;
            _providerName = providerName;

            // Cache size
            string cacheSizeString;
            _cacheSize = DefaultCacheSize;
            if (config.Properties.TryGetValue(CacheSizeParam, out cacheSizeString))
            {
                if (!int.TryParse(cacheSizeString, out _cacheSize))
                    throw new ArgumentException($"{CacheSizeParam} invalid.  Must be int");
            }

            // # queues
            string numQueuesString;
            _numQueues = DefaultNumQueues;
            if (config.Properties.TryGetValue(NumQueuesParam, out numQueuesString))
            {
                if (!int.TryParse(numQueuesString, out _numQueues))
                    throw new ArgumentException($"{NumQueuesParam} invalid.  Must be int");
            }

            // server
            string server;
            _server = DefaultServer;
            if (config.Properties.TryGetValue(ServerParam, out server))
            {
                if (server == "")
                    throw new ArgumentException($"{DefaultServer} invalid. Must not be empty");
                _server = server;
            }

            // db
            string dbNum;
            _databaseNum = DefaultRedisDb;
            if (config.Properties.TryGetValue(RedisDbParam, out dbNum))
            {
                if (!int.TryParse(dbNum, out _databaseNum))
                    throw new ArgumentException($"{RedisDbParam} invalid.  Must be int");
                if (_databaseNum > 15 || _databaseNum < 0)
                    throw new ArgumentException($"{RedisDbParam} invalid.  Must be from 0 to 15");
            }

            _streamQueueMapper = new HashRingBasedStreamQueueMapper(_numQueues, providerName);
            // FIXME
             ConnectionMultiplexer.ConnectAsync(_server).ContinueWith((t) =>
            {
                _connection = t.Result;
                _db = _connection.GetDatabase(_databaseNum);
                _adapterCache = new QueueAdapterCacheRedis(_logger, _db);
            });
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new PipeQueueAdapter(_logger, GetStreamQueueMapper(), _providerName);
            return Task.FromResult<IQueueAdapter>(adapter);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return _adapterCache;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return _streamQueueMapper;
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return Task.FromResult<IStreamFailureHandler>(new MyStreamFailureHandler(_logger));
        }
    }
}