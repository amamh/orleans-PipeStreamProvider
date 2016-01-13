using System;
using System.Threading.Tasks;
using Orleans;

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

            BenchmarkRead();

            Console.ReadLine();
        }

        private static void BenchmarkRead()
        {
            var testObserver = new BenchmarkObserver();
            testObserver.Subscribe().Wait();
        }

        private static void TestRead()
        {
            //var testObserver = GrainClient.GrainFactory.GetGrain<ITestObserver>(0);
            var testObserver = new TestObserver();
            testObserver.Subscribe1().Wait();

            testObserver = new TestObserver();
            testObserver.Subscribe2().Wait();
        }
    }
}
