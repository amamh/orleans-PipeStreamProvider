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
using NetMQ;
using NetMQ.Sockets;

namespace PipeStreamProvider
{
    public class PipeQueueAdapter : IQueueAdapter
    {
        private IStreamQueueMapper _streamQueueMapper;
        //private readonly ConcurrentDictionary<QueueId, Queue<byte[]>> _queues = new ConcurrentDictionary<QueueId, Queue<byte[]>>();

        private NetMQContext _context;
        private PushSocket _socket;

        public PipeQueueAdapter(IStreamQueueMapper streamQueueMapper, string name)
        {
            _streamQueueMapper = streamQueueMapper;
            Name = name;
            _context = NetMQContext.Create();
            _socket = _context.CreatePushSocket();
            _socket.Connect("tcp://127.0.0.1:1234");
        }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            if (events == null)
            {
                throw new ArgumentNullException(nameof(events), "Trying to QueueMessageBatchAsync null data.");
            }

            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            //Queue<byte[]> queue;
            //if (!_queues.TryGetValue(queueId, out queue))
            //{
            //    var tmpQueue = new Queue<byte[]>();
            //    queue = _queues.GetOrAdd(queueId, tmpQueue);
            //}


            var eventsAsObjects = events.Cast<object>().ToList();

            var container = new PipeQueueAdapterBatchContainer(streamGuid, streamNamespace, eventsAsObjects, requestContext);

            var bytes = SerializationManager.SerializeToByteArray(container);

            _socket.SendMoreFrame(queueId.ToString()).SendFrame(bytes);

            //queue.Enqueue(bytes);

            return TaskDone.Done;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            //Queue<byte[]> queue;
            //if (!_queues.TryGetValue(queueId, out queue))
            //{
            //    var tmpQueue = new Queue<byte[]>();
            //    queue = _queues.GetOrAdd(queueId, tmpQueue);
            //}

            return new PipeQueueAdapterReceiver(queueId);
        }

        public string Name { get; }
        public bool IsRewindable { get; } = true;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;
    }
}