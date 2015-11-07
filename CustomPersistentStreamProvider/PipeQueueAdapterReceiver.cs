using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace StellaStreams
{
    public class PipeQueueAdapterReceiver : IQueueAdapterReceiver
    {
        public QueueId Id { get; }
        private readonly ConcurrentDictionary<QueueId, Queue<byte[]>> _queues;
        private long _sequenceId;
        public PipeQueueAdapterReceiver(QueueId queueId, ConcurrentDictionary<QueueId, Queue<byte[]>> queues)
        {
            //_messages = queue;
            Id = queueId;
            _queues = queues;
        }

        public Task Initialize(TimeSpan timeout)
        {
            //throw new NotImplementedException();
            return TaskDone.Done;
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            if (!_queues.ContainsKey(Id))
                return Task.FromResult<IList<IBatchContainer>>(new List<IBatchContainer>());

            var queue = _queues[Id];

            var listOfMessages = new List<byte[]>();
            for (var i = 0; i < maxCount; i++)
                if (queue.Count > 0)
                    listOfMessages.Add(queue.Dequeue());

            var list = (from m in listOfMessages select SerializationManager.DeserializeFromByteArray<PipeQueueAdapterBatchContainer>(m));
            var pipeQueueAdapterBatchContainers = list as IList<PipeQueueAdapterBatchContainer> ?? list.ToList();
            foreach (var batchContainer in pipeQueueAdapterBatchContainers)
                batchContainer.EventSequenceToken = new EventSequenceToken(_sequenceId++);

            return Task.FromResult<IList<IBatchContainer>>(pipeQueueAdapterBatchContainers.ToList<IBatchContainer>());
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            //Console.WriteLine($"Delivered {messages.Count}, last one has token {messages.Last().SequenceToken}");
            return TaskDone.Done;
        }

        public Task Shutdown(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }
    }
}