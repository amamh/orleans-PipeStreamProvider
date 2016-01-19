using System;
using System.Threading.Tasks;
using GrainInterfaces;
using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Client
{
    public class TestObserver : IAsyncObserver<int>
    {
        public async Task Subscribe(bool recover = false)
        {
            var providerName = "PSProvider";
            var streamId = new Guid("00000000-0000-0000-0000-000000000000");

            var provider = GrainClient.GetStreamProvider(providerName);
            var stream = provider.GetStream<int>(streamId, "GlobalNamespace");
            await stream.SubscribeAsync(this, recover ? new PipeStreamProvider.TimeSequenceToken(DateTime.UtcNow - TimeSpan.FromSeconds(5)) : null);
        }

        public Task OnNextAsync(int item, StreamSequenceToken token = null)
        {
            Console.WriteLine($"{item}");
            return TaskDone.Done;
        }

        public Task OnCompletedAsync()
        {
            Console.WriteLine("Done");
            return TaskDone.Done;
        }

        public Task OnErrorAsync(Exception ex)
        {
            Console.WriteLine(ex);
            return TaskDone.Done;
        }
    }
}