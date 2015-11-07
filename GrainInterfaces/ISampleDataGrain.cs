using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using Orleans.Streams;

namespace GrainInterfaces
{
    public interface ISampleDataGrain : IGrainWithIntegerKey
    {
        Task SetRandomData(int random);
        Task<IAsyncStream<int>> GetStream();

        Task<Immutable<LinkedList<int>>> Subscribe(IAsyncObserver<int> observer, bool recover);
    }
}
