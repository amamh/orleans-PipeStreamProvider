using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace PipeStreamProvider.PhysicalQueues.Redis
{
    public class GenericQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly Logger _logger;
        public QueueId Id { get; }
        private long _sequenceId;
        private readonly IProviderQueue _queueProvider;

        public GenericQueueAdapterReceiver(Logger logger, QueueId queueid, IProviderQueue queueProvider)
        {
            _queueProvider = queueProvider;
            _logger = logger;

            Id = queueid;
        }

        public Task Initialize(TimeSpan timeout)
        {
            return TaskDone.Done;
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var listOfMessages = new List<byte[]>();

            var listLength = _queueProvider.Length(Id);
            var max = Math.Max(maxCount, listLength);

            for (var i = 0; i < max; i++)
            {
                var nextMsg = _queueProvider.Dequeue(Id);
                listOfMessages.Add(nextMsg);
            }

            var list = (from m in listOfMessages select SerializationManager.DeserializeFromByteArray<PipeQueueAdapterBatchContainer>(m));
            var pipeQueueAdapterBatchContainers = list as IList<PipeQueueAdapterBatchContainer> ?? list.ToList();
            foreach (var batchContainer in pipeQueueAdapterBatchContainers)
                batchContainer.SimpleSequenceToken = new SimpleSequenceToken(_sequenceId++);

            _logger.AutoVerbose($"Read {pipeQueueAdapterBatchContainers.Count} batch containers");
            return Task.FromResult<IList<IBatchContainer>>(pipeQueueAdapterBatchContainers.ToList<IBatchContainer>());
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            var count = messages == null ? 0 : messages.Count;
            var lastToken = messages?.Count != 0 ? messages?.Last()?.SequenceToken.ToString() : "--";
            _logger.AutoVerbose($"Delivered {count}, last one has token {lastToken}");
            return TaskDone.Done;
        }

        public Task Shutdown(TimeSpan timeout)
        {
            _logger.AutoInfo("Receiver requested to shutdown.");
            return TaskDone.Done;
        }
    }
}
