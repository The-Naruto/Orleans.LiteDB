using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Clustering.LiteDB;
using Orleans.Clustering.LiteDB.Options;
using Orleans.Hosting;
using Orleans.Providers;
[assembly: RegisterProvider("LiteDB", "Clustering", "Silo", typeof(LiteDBClusteringProviderBuilder))]
[assembly: RegisterProvider("LiteDB", "Clustering", "Client", typeof(LiteDBClusteringProviderBuilder))]

namespace Orleans.Clustering.LiteDB
{


    internal class LiteDBClusteringProviderBuilder : IProviderBuilder<ISiloBuilder>, IProviderBuilder<IClientBuilder>
    {
        public void Configure(ISiloBuilder builder, string name, IConfigurationSection configurationSection)
        {
            builder.UseLiteDBClustering((OptionsBuilder<LiteDBClusteringSiloOptions> optionsBuilder) => optionsBuilder.Configure<IServiceProvider>((options, services) =>
            {

                var connectionString = configurationSection[nameof(options.ConnectionString)];
                var connectionName = configurationSection["ConnectionName"];
                if (string.IsNullOrEmpty(connectionString) && !string.IsNullOrEmpty(connectionName))
                {
                    connectionString = services.GetRequiredService<IConfiguration>().GetConnectionString(connectionName);
                }

                if (!string.IsNullOrEmpty(connectionString))
                {
                    options.ConnectionString = connectionString;
                }
            }));
        }

        public void Configure(IClientBuilder builder, string name, IConfigurationSection configurationSection)
        {
            builder.UseLiteDBClustering((OptionsBuilder<LiteDBClusteringClientOptions> optionsBuilder) => optionsBuilder.Configure<IServiceProvider>((options, services) =>
            {


                var connectionString = configurationSection[nameof(options.ConnectionString)];
                var connectionName = configurationSection["ConnectionName"];
                if (string.IsNullOrEmpty(connectionString) && !string.IsNullOrEmpty(connectionName))
                {
                    connectionString = services.GetRequiredService<IConfiguration>().GetConnectionString(connectionName);
                }

                if (!string.IsNullOrEmpty(connectionString))
                {
                    options.ConnectionString = connectionString;
                }
            }));
        }
    }
}