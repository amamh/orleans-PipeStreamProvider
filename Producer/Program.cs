using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GrainInterfaces;
using Orleans;

namespace Producer
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
                    Task.Delay(100).Wait();
                }
            }

            WriteSome();
        }

        static void WriteSome()
        {
            var providerName = "PSProvider";
            var streamId = new Guid("00000000-0000-0000-0000-000000000000");

            var provider = GrainClient.GetStreamProvider(providerName);
            var stream = provider.GetStream<int>(streamId, "GlobalNamespace");
            //var i = 0;
            for (int i = 0; i < 1000; i++)
            {
                Task.Delay(500).Wait();
                stream.OnNextAsync(i);
                Console.WriteLine($"Writing....: {i}\t\t{DateTime.UtcNow.Millisecond}");
            }
        }

        static void WriteALot()
        {
            var providerName = "PSProvider";
            var streamId = new Guid("00000000-0000-0000-0000-000000000000");

            var provider = GrainClient.GetStreamProvider(providerName);
            var stream = provider.GetStream<int>(streamId, "GlobalNamespace");

            var start = DateTime.UtcNow;
            var t1 = start;
            DateTime t2;

            for (int i = 0; i < 1000000; i++)
            {
                stream.OnNextAsync(i);

                if (i % 10000 == 0)
                {
                    t2 = DateTime.UtcNow;
                    Console.WriteLine($"New mark set. Time since last mark: {(t2 - t1).TotalMilliseconds}");
                    t1 = t2;
                }
            }

            var end = DateTime.UtcNow;

            Console.WriteLine($"{(end - start).TotalMilliseconds} ms");

            //stream.OnCompletedAsync().Wait();
        }
    }
}
