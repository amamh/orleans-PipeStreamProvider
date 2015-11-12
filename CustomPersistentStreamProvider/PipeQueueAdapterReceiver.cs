using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using NetMQ.Sockets;
using NetMQ;

namespace PipeStreamProvider
{
    public class PipeQueueAdapterReceiver : IQueueAdapterReceiver
    {
        public QueueId Id { get; }
        private readonly Queue<byte[]> _queue;
        private long _sequenceId;

        private readonly NetMQContext _context;
        private readonly PullSocket _socket;

        public PipeQueueAdapterReceiver(QueueId queueid)
        {
            //_messages = queue;
            _context = NetMQContext.Create();
            _socket = _context.CreatePullSocket();
            _socket.Connect("tcp://127.0.0.1:5678");

            Id = queueid;
        }

        public Task Initialize(TimeSpan timeout)
        {
            //throw new NotImplementedException();
            return TaskDone.Done;
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var listOfMessages = new List<byte[]>();
            for (var i = 0; i < maxCount; i++)
            {
                string topic;
                if (!_socket.TryReceiveFrameString(TimeSpan.FromMilliseconds(10), out topic)) continue;

                if (topic == Id.ToString())
                {
                    byte[] msg;
                    if (_socket.TryReceiveFrameBytes(out msg))
                        if (msg != null && msg.Length != 0)
                            listOfMessages.Add(msg);
                }
                else if (string.IsNullOrWhiteSpace(topic))
                    break;
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