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

            var i = 0;
            while (true)
            {
                Task.Delay(500).Wait();
                var grain = GrainClient.GrainFactory.GetGrain<ISampleDataGrain>(0);
                grain.SetRandomData(i).Wait();
                Console.WriteLine("Writing....: " + i);
                i++;
            }
        }
    }
}
