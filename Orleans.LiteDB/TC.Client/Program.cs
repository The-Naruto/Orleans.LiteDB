// See https://aka.ms/new-console-template for more information
using Microsoft.CodeAnalysis.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Clustering.LiteDB;
using Orleans.Clustering.LiteDB.Options;
using Orleans.Configuration;
using TC.Client;


Console.WriteLine("Hello, World!");




var hostBuilder = Host.CreateDefaultBuilder();



hostBuilder.UseOrleansClient(clientbuilder =>
{
    clientbuilder.UseLiteDBClustering((LiteDBClusteringClientOptions options) =>

    {
        options.ConnectionString = "Filename= D:\\LiteDBClustering2.db;connection=Shared;";
    })
       .Configure<ClusterOptions>(options =>
       {
           //options.ClusterId = "dev";
           options.ServiceId = "LinkSilo";
           options.ClusterId = ClusterOptions.DefaultClusterId;
       });



});


hostBuilder.ConfigureServices(services =>
    services.AddHostedService<ClientMannager>()

    );



await hostBuilder.RunConsoleAsync();
