using System;
using System.Threading.Tasks;
using GrainInterfaces;
using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Client
{
    public class BenchmarkObserver : IAsyncObserver<int>
    {
        public async Task Subscribe()
        {
            var providerName = "PSProvider";
            var streamId = new Guid("00000000-0000-0000-0000-000000000000");

            var provider = GrainClient.GetStreamProvider(providerName);
            var stream = provider.GetStream<int>(streamId, "GlobalNamespace");
            await stream.SubscribeAsync(this, new PipeStreamProvider.SimpleSequenceToken(0));
        }

        bool firstMessage = false;
        DateTime t1, t2;

        public Task OnNextAsync(int item, StreamSequenceToken token = null)
        {
            if (!firstMessage)
            {
                firstMessage = true;
                t1 = DateTime.UtcNow;
            }

            //Console.WriteLine($"{item}");

            if (item % 10000 == 0)
            {
                t2 = DateTime.UtcNow;
                Console.WriteLine($"New mark set. Time since last mark: {(t2 - t1).TotalMilliseconds}");
                t1 = t2;
            }

            return TaskDone.Done;
        }

        public Task OnCompletedAsync()
        {
            return TaskDone.Done;
        }

        public Task OnErrorAsync(Exception ex)
        {
            Console.WriteLine(ex);
            return TaskDone.Done;
        }
    }
}