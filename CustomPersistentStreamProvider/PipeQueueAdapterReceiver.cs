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
using StackExchange.Redis;

namespace PipeStreamProvider
{
    public class PipeQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly Logger _logger;
        private readonly IDatabase _database;
        private readonly string _redisListName;
        public QueueId Id { get; }
        private long _sequenceId;


        public PipeQueueAdapterReceiver(Logger logger, QueueId queueid, IDatabase database, string redisListName)
        {
            _logger = logger;
            _database = database;
            _redisListName = redisListName;

            Id = queueid;
        }

        public Task Initialize(TimeSpan timeout)
        {
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
                    _logger.AutoError($"Couldn't read from Redis list {_redisListName}, exception: {exception}");
                    break;
                }
            }

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
            Console.WriteLine("Receiver shutting");
            return TaskDone.Done;
        }
    }
}