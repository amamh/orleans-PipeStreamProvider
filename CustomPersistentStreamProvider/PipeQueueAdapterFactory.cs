using System;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace PipeStreamProvider
{
    public class PipeQueueAdapterFactory : IQueueAdapterFactory
    {
        private const string CacheSizeParam = "CacheSize";
        private const int DefaultCacheSize = 4096;

        private const string NumQueuesParam = "NumQueues";
        private const int DefaultNumQueues = 8; // keep as power of 2.

        private string _providerName;
        private Logger _logger;
        private int _cacheSize;
        private int _numQueues;
        private HashRingBasedStreamQueueMapper _streamQueueMapper;
        private IQueueAdapterCache _adapterCache;

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

            _streamQueueMapper = new HashRingBasedStreamQueueMapper(_numQueues, providerName);
            _adapterCache = new MySimpleQueueAdapterCache(this, _cacheSize, _logger);
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