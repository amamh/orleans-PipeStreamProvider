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

namespace PipeStreamProvider
{
    public class PipeQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly Logger _logger;
        private readonly Queue<byte[]> _queue;
        public QueueId Id { get; }
        private long _sequenceId;


        public PipeQueueAdapterReceiver(Logger logger, QueueId queueid, Queue<byte[]> queue)
        {
            _logger = logger;
            Id = queueid;
            _queue = queue;
        }

        public Task Initialize(TimeSpan timeout)
        {
            return TaskDone.Done;
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var listOfMessages = new List<byte[]>();

            var min = Math.Min(maxCount, _queue.Count);
            for (var i = 0; i < min; i++)
                listOfMessages.Add(_queue.Dequeue());

            var list = (from m in listOfMessages select SerializationManager.DeserializeFromByteArray<PipeQueueAdapterBatchContainer>(m));
            var pipeQueueAdapterBatchContainers = list as IList<PipeQueueAdapterBatchContainer> ?? list.ToList();
            // Set stream sequence tokens
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