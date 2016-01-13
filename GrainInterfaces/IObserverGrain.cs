using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;

namespace GrainInterfaces
{
    /// <summary>
    /// Orleans grain communication interface ITestObserver
    /// </summary>
    public interface IObserverGrain : Orleans.IGrainWithIntegerKey, IAsyncObserver<int>
    {
        Task Subscribe();
    }
}
