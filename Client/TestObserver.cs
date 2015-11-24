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
        public async Task Subscribe1()
        {
            var ccGrain = GrainClient.GrainFactory.GetGrain<ISampleDataGrain>(0);
            var stream = await ccGrain.GetStream();
            await stream.SubscribeAsync(this, new PipeStreamProvider.SimpleSequenceToken(0));
        }

        public async Task Subscribe2()
        {
            var ccGrain = GrainClient.GrainFactory.GetGrain<ISampleDataGrain>(0);
            //var stream = await ccGrain.GetStream();

            var details = await ccGrain.GetStreamDetails();
            var providerName = details.Item1;
            var nameSpace = details.Item2;
            var guid = details.Item3;

            var provider = GrainClient.GetStreamProvider(providerName);
            var stream = provider.GetStream<int>(guid, nameSpace);

            await stream.SubscribeAsync(this, new PipeStreamProvider.SimpleSequenceToken(0));
        }

        public Task OnNextAsync(int item, StreamSequenceToken token = null)
        {
            Console.WriteLine($"{item}\t\t{DateTime.UtcNow.Millisecond}");
            return TaskDone.Done;
        }

        public Task OnCompletedAsync()
        {
            throw new NotImplementedException();
        }

        public Task OnErrorAsync(Exception ex)
        {
            throw new NotImplementedException();
        }
    }
}