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

namespace PipeStreamProvider.PhysicalQueues
{
    public class GenericQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly Logger _logger;
        public QueueId Id { get; }
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

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var listOfMessages = new List<byte[]>();

            var listLength = await _queueProvider.Length(Id);
            var max = Math.Min(maxCount, listLength);

            for (var i = 0; i < max; i++)
            {
                var nextMsg = await _queueProvider.Dequeue(Id);
                if (nextMsg != null)
                    listOfMessages.Add(nextMsg);
                else
                    _logger.AutoWarn("The queue returned a null message. This shouldn't happen. Ignored.");
            }

            if (listOfMessages.Count == 0)
                return null;

            _logger.AutoVerbose($"Read {listOfMessages.Count} batch containers");
            var list = (from m in listOfMessages select SerializationManager.DeserializeFromByteArray<IBatchContainer>(m)).ToList();

            return list;
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            var count = messages?.Count ?? 0;
            if (count == 0)
                return TaskDone.Done;
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
