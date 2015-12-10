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

            var grain = GrainClient.GrainFactory.GetGrain<ISampleDataGrain>(0);
            var stream = grain.GetStream().Result;
            //var i = 0;
            for (int i = 0; i < 1000; i++)
            {
                Task.Delay(500).Wait();
                stream.OnNextAsync(i);
                Console.WriteLine($"Writing....: {i}\t\t{DateTime.UtcNow.Millisecond}");
            }
        }
    }
}
