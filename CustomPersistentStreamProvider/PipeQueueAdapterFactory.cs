using System;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using StackExchange.Redis;

namespace PipeStreamProvider
{
    public class PipeQueueAdapterFactory : IQueueAdapterFactory
    {
        private const string CACHE_SIZE_PARAM = "CacheSize";
        private const int DEFAULT_CACHE_SIZE = 4096;

        private const string NUM_QUEUES_PARAM = "NumQueues";
        private const int DEFAULT_NUM_QUEUES = 8; // keep as power of 2.

        private const string SERVER_PARAM = "Server";
        private const string DEFAULT_SERVER = "localhost:6379";

        private const string REDIS_DB_PARAM = "RedisDb";
        private const int DEFAULT_REDIS_DB = -1;

        private string _providerName;
        private Logger _logger;
        private int _cacheSize;
        private int _numQueues;
        private string _server;
        private HashRingBasedStreamQueueMapper _streamQueueMapper;
        private IQueueAdapterCache _adapterCache;
        private ConnectionMultiplexer _connection;
        private int _databaseNum;

        public void Init(IProviderConfiguration config, string providerName, Logger logger)
        {
            _logger = logger;
            _providerName = providerName;

            // Cache size
            string cacheSizeString;
            _cacheSize = DEFAULT_CACHE_SIZE;
            if (config.Properties.TryGetValue(CACHE_SIZE_PARAM, out cacheSizeString))
            {
                if (!int.TryParse(cacheSizeString, out _cacheSize))
                    throw new ArgumentException($"{CACHE_SIZE_PARAM} invalid.  Must be int");
            }

            // # queues
            string numQueuesString;
            _numQueues = DEFAULT_NUM_QUEUES;
            if (config.Properties.TryGetValue(NUM_QUEUES_PARAM, out numQueuesString))
            {
                if (!int.TryParse(numQueuesString, out _numQueues))
                    throw new ArgumentException($"{NUM_QUEUES_PARAM} invalid.  Must be int");
            }

            // server
            string server;
            _server = DEFAULT_SERVER;
            if (config.Properties.TryGetValue(SERVER_PARAM, out server))
            {
                if (server == "")
                    throw new ArgumentException($"{DEFAULT_SERVER} invalid. Must not be empty");
                _server = server;
            }

            // db
            string dbNum;
            _databaseNum = DEFAULT_REDIS_DB;
            if (config.Properties.TryGetValue(REDIS_DB_PARAM, out dbNum))
            {
                if (!int.TryParse(dbNum, out _databaseNum))
                    throw new ArgumentException($"{REDIS_DB_PARAM} invalid.  Must be int");
                if (_databaseNum > 15 || _databaseNum < 0)
                    throw new ArgumentException($"{REDIS_DB_PARAM} invalid.  Must be from 0 to 15");
            }


            _streamQueueMapper = new HashRingBasedStreamQueueMapper(_numQueues, providerName);
            _adapterCache = new MySimpleQueueAdapterCache(this, _cacheSize, _logger);
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new PipeQueueAdapter(_logger, GetStreamQueueMapper(), _providerName, _server, _databaseNum, "orleans");
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