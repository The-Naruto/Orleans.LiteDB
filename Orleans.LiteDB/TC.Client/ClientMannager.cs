
using Microsoft.Extensions.Hosting;
using Orleans;
using TC.Grains;

namespace TC.Client
{
    public class ClientMannager : BackgroundService
    {

        private readonly IClusterClient clusterClient;


        public ClientMannager(IClusterClient clusterClient)
        {
            this.clusterClient = clusterClient;
        }


        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {

            IHelloWorld[] helloWorlds = new IHelloWorld[10];

            for (int i = 0; i < 10; i++)
            {
                IHelloWorld grain = clusterClient.GetGrain<IHelloWorld>("user" + i);
                helloWorlds[i] = grain;


            }



            return Task.Run(async () =>
            {

                while (!stoppingToken.IsCancellationRequested)
                {
                    var idx = Random.Shared.Next(10);
                    var times = Random.Shared.Next(10);

                    helloWorlds[idx].SetTimes(times);
                    helloWorlds[idx].SayHello();
                   await  Task.Delay(1000);
                }



            });


        }
    }
}
