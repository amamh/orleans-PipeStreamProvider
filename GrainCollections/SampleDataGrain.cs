using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using GrainInterfaces;
using Orleans;
using Orleans.Concurrency;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace GrainCollections
{
    public class SampleDataGrain : Orleans.Grain, IDataGrain
    {
        private Guid _streamGuid;
        private IAsyncStream<int> _stream;
        private LinkedList<int> _historicalData;
        public const string ProviderToUse = "PSProvider";
        //public const string ProviderToUse = "SMSProvider";
        public const string StreamNamespace = "GlobalNamespace";

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();

            _historicalData = new LinkedList<int>();
            _streamGuid = Guid.NewGuid();
            var streamProvider = GetStreamProvider(ProviderToUse);
            _stream = streamProvider.GetStream<int>(_streamGuid, StreamNamespace);
        }

        public Task SetRandomData(int random)
        {
            _historicalData.AddLast(random);
            _stream.OnNextAsync(random);
            return TaskDone.Done;
        }

        public Task<IAsyncStream<int>> GetStream()
        {
            return Task.FromResult(_stream);
        }

        public async Task Subscribe(IAsyncObserver<int> observer, bool recover = false)
        {
            await _stream.SubscribeAsync(observer, recover ? new PipeStreamProvider.TimeSequenceToken(DateTime.UtcNow - TimeSpan.FromSeconds(5)) : null);
        }
        public Task<Tuple<string, string, Guid>> GetStreamDetails()
        {
            return Task.FromResult(Tuple.Create(ProviderToUse, StreamNamespace, _streamGuid));
        }
    }
}
