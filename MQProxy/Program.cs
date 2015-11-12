using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetMQ;

namespace MQProxy
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var context = NetMQContext.Create())
            using (var xpubSocket = context.CreateXPublisherSocket())
            using (var xsubSocket = context.CreateXSubscriberSocket())
            {
                xpubSocket.Bind("tcp://127.0.0.1:1234");
                xsubSocket.Bind("tcp://127.0.0.1:5678");

                Console.WriteLine("Intermediary started, and waiting for messages");

                // proxy messages between frontend / backend
                var proxy = new Proxy(xsubSocket, xpubSocket);

                // blocks indefinitely
                proxy.Start();
            }
            Console.ReadLine();
        }
    }
}
