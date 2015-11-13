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

namespace PipeStreamProvider
{
    public class PipeQueueAdapter : IQueueAdapter
    {
        private readonly IStreamQueueMapper _streamQueueMapper;
        private ConnectionMultiplexer _connection;
        private readonly int _databaseNum;
        private readonly string _server;
        private IDatabase _database;
        private readonly string _redisListBaseName;
        //private readonly ConcurrentDictionary<QueueId, Queue<byte[]>> _queues = new ConcurrentDictionary<QueueId, Queue<byte[]>>();


        public PipeQueueAdapter(IStreamQueueMapper streamQueueMapper, string name, string server, int database, string redisListBaseName)
        {
            _streamQueueMapper = streamQueueMapper;
            _databaseNum = database;
            _server = server;
            _redisListBaseName = redisListBaseName;


            ConnectionMultiplexer.ConnectAsync(_server).ContinueWith(task =>
            {
                _connection = task.Result;
                _database = _connection.GetDatabase(_databaseNum);
            });

            Name = name;
        }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            if (_database == null)
                return TaskDone.Done;

            if (events == null)
            {
                throw new ArgumentNullException(nameof(events), "Trying to QueueMessageBatchAsync null data.");
            }

            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            var redisListName = GetRedisListName(queueId);

            var eventsAsObjects = events.Cast<object>().ToList();

            var container = new PipeQueueAdapterBatchContainer(streamGuid, streamNamespace, eventsAsObjects, requestContext);

            var bytes = SerializationManager.SerializeToByteArray(container);

            _database.ListLeftPush(redisListName, bytes);

            return TaskDone.Done;
        }

        private string GetRedisListName(QueueId queueId)
        {
            return _redisListBaseName + queueId;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return new PipeQueueAdapterReceiver(queueId, _database, GetRedisListName(queueId));
        }

        public string Name { get; }
        public bool IsRewindable { get; } = true;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;
    }
}