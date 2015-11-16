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
            foreach (var batchContainer in pipeQueueAdapterBatchContainers)
                batchContainer.EventSequenceToken = new EventSequenceToken(_sequenceId++);

            _logger.AutoVerbose($"Read {pipeQueueAdapterBatchContainers.Count} batch containers");
            return Task.FromResult<IList<IBatchContainer>>(pipeQueueAdapterBatchContainers.ToList<IBatchContainer>());
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            _logger.AutoVerbose($"Delivered {messages.Count}, last one has token {messages.Last().SequenceToken}");
            return TaskDone.Done;
        }

        public Task Shutdown(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }
    }
}