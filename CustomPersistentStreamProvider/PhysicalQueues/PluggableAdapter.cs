using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;
using Orleans.Providers;

namespace PipeStreamProvider.PhysicalQueues
{
    public class GenericQueueAdapter : IQueueAdapter
    {
        private Logger _logger;
        private IStreamQueueMapper _streamQueueMapper;
        private readonly IProviderConfiguration _config;
        private readonly IProviderQueue _queueProvider;

        public GenericQueueAdapter(Logger logger, IStreamQueueMapper streamQueueMapper, string providerName, IProviderConfiguration config, IProviderQueue queueProvider, int numOfQueues)
        {
            Name = providerName; // WTF: If you set the name to anything else, the client won't receive any messages !?????
            _config = config;
            _logger = logger;
            _streamQueueMapper = streamQueueMapper;
            _queueProvider = queueProvider;

            queueProvider.Init(_logger, _config, providerName, numOfQueues);
        }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {

            if (events == null)
            {
                throw new ArgumentNullException(nameof(events), "Trying to QueueMessageBatchAsync null data.");
            }

            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);

            var eventsAsObjects = events.Cast<object>().ToList();

            var container = new PipeQueueAdapterBatchContainer(streamGuid, streamNamespace, eventsAsObjects, requestContext);

            var bytes = SerializationManager.SerializeToByteArray(container);

            _queueProvider.Enqueue(queueId, bytes);

            return TaskDone.Done;
        }


        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return new GenericQueueAdapterReceiver(_logger, queueId, _queueProvider);
        }

        public string Name { get; }
        public bool IsRewindable { get; } = true;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;
    }
}
