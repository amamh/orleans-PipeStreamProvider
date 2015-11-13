using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;

namespace PipeStreamProvider
{
    public class PipeQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly IDatabase _database;
        private readonly string _redisListName;
        public QueueId Id { get; }
        private readonly Queue<byte[]> _queue;
        private long _sequenceId;


        public PipeQueueAdapterReceiver(QueueId queueid, IDatabase database, string redisListName)
        {
            _database = database;
            _redisListName = redisListName;

            Id = queueid;
        }

        public Task Initialize(TimeSpan timeout)
        {
            //throw new NotImplementedException();
            return TaskDone.Done;
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            if (!_database.KeyExists(_redisListName))
                return Task.FromResult<IList<IBatchContainer>>(new List<IBatchContainer>());

            var listOfMessages = new List<byte[]>();

            var listLength = _database.ListLength(_redisListName);
            var max = Math.Max(maxCount, listLength);
            
            for (var i = 0; i < max; i++)
            {
                try
                {
                    var nextMsg = _database.ListRightPop(_redisListName);
                    if (!nextMsg.IsNull)
                        listOfMessages.Add(nextMsg);
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"Receiver exception: {exception.ToString()}");
                    break;
                }
            }

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