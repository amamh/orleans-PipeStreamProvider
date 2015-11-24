using System;
using System.Threading.Tasks;
using GrainInterfaces;
using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace GrainCollections
{
    /// <summary>
    /// Grain implementation class Grain1.
    /// </summary>
    public class TestObserver : Orleans.Grain, ITestObserver
    {
        private StreamSubscriptionHandle<int> _handler;
        public async Task Subscribe()
        {
            var ccGrain = GrainFactory.GetGrain<ISampleDataGrain>(0);
            //var stream = await ccGrain.GetStream();
            //_handler = await stream.SubscribeAsync(this, new SimpleSequenceToken(0));

            //var details = await ccGrain.GetStreamDetails();
            //var providerName = details.Item1;
            //var nameSpace = details.Item2;
            //var guid = details.Item3;

            //var provider = GrainClient.GetStreamProvider(providerName);
            //var stream = provider.GetStream<int>(guid, nameSpace);
            var stream = await ccGrain.GetStream();
            _handler = await stream.SubscribeAsync(this, null);
        }

        public override async Task OnDeactivateAsync()
        {
            if (_handler != null)
                await _handler.UnsubscribeAsync();
        }

        public Task OnNextAsync(int item, StreamSequenceToken token = null)
        {
            Console.WriteLine(item);
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
