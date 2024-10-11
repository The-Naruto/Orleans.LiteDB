// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Hosting;
using Orleans.Clustering.LiteDB;
using Orleans.Clustering.LiteDB.Options;
using Orleans.Configuration;
using System.Net;
using System.Net.Sockets;

var hostBuilder = Host.CreateDefaultBuilder();



hostBuilder.UseOrleans(siloBuilder =>
     siloBuilder
               //.UseLocalhostClustering()
                //
                .UseLiteDBClustering((LiteDBClusteringSiloOptions option) =>
                 {
                     option.ConnectionString = "Filename= D:\\LiteDBClustering2.db;Mode=Shared;";
                 })
                
                .Configure<ClusterOptions>(options =>
                {
                    //options.ClusterId = "dev";
                    options.ServiceId = "LinkSilo";
                    options.ClusterId = ClusterOptions.DefaultClusterId;
                })

                 .Configure<EndpointOptions>(
                        options =>
                        {

                            options.SiloPort = GetAvailablePort();
                            // Port to use for the gateway
                            options.GatewayPort = GetAvailablePort();
                            //只接收本地请求
                            options.AdvertisedIPAddress = IPAddress.Loopback;
                            // The socket used for silo-to-silo will bind to this endpoint
                            // options.GatewayListeningEndpoint = new IPEndPoint(IPAddress.Loopback, gatewayPort);
                            // // The socket used by the gateway will bind to this endpoint
                            // options.SiloListeningEndpoint = new IPEndPoint(IPAddress.Loopback, siloPort);

                        })
                 .UseDashboard(o =>
                 {
                     o.CounterUpdateIntervalMs = 10 * 1000;
                     o.Port = GetAvailablePort();
                     Console.WriteLine(">>>>>>>>>>>>>>>>>>>>dashboard port:" + o.Port);
                     //  o.HostSelf = true; 
                 })



                );



await hostBuilder.RunConsoleAsync();


static int GetAvailablePort()
{
    TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    int port = ((IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
}
