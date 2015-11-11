using System;
using System.Threading.Tasks;
using GrainInterfaces;
using Orleans;
using Orleans.Streams;
using Orleans.Providers.Streams.Common;

namespace Client
{
    class Program
    {
        static void Main(string[] args)
        {
            while (true)
            {
                try
                {
                    GrainClient.Initialize();
                    break;
                }
                catch (Exception)
                {
                    Task.Delay(500).Wait();
                }
            }
            Console.WriteLine("Waiting");
            Task.Delay(2000).Wait();
            Console.WriteLine("Starting");
            //var testObserver = GrainClient.GrainFactory.GetGrain<ITestObserver>(0);
            var testObserver = new TestObserver();
            testObserver.Subscribe().Wait();
            Console.ReadLine();
        }

        [Serializable]
        public class TestObserver : IAsyncObserver<int>
        {
            public async Task Subscribe()
            {
                var ccGrain = GrainClient.GrainFactory.GetGrain<ISampleDataGrain>(0);

                var details = await ccGrain.GetStreamDetails();
                var providerName = details.Item1;
                var nameSpace = details.Item2;
                var guid = details.Item3;

                var provider = GrainClient.GetStreamProvider(providerName);
                var stream = provider.GetStream<int>(guid, nameSpace);
                //var stream = await ccGrain.GetStream();
                await stream.SubscribeAsync(this, null);
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
}
