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
            var stream = await ccGrain.GetStream();
            //var reference = this.AsReference<ITestObserver>();
            _handler = await stream.SubscribeAsync(this, new EventSequenceToken(0));
        }

        public override async Task OnDeactivateAsync()
        {
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
