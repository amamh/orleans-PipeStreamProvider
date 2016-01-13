using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using Orleans.Streams;

namespace GrainInterfaces
{
    public interface IDataGrain : IGrainWithIntegerKey
    {
        Task SetRandomData(int random);
        Task<IAsyncStream<int>> GetStream();

        Task Subscribe(IAsyncObserver<int> observer, bool recover = false);

        Task<Tuple<string, string, Guid>> GetStreamDetails();
    }
}
