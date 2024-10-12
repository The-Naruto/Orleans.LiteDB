using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Clustering.LiteDB.Messaging;
using Orleans.Clustering.LiteDB.Options;
using Orleans.Hosting;
using Orleans.Messaging;

namespace Orleans.Clustering.LiteDB
{
    public static class LiteDBHostingExtensions
    {
        /// <summary>
        /// Configures this silo to use ADO.NET for clustering. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloBuilder"/>.
        /// </returns>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static ISiloBuilder UseLiteDBClustering(
            this ISiloBuilder builder,
            Action<LiteDBClusteringSiloOptions> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    if (configureOptions != null)
                    {
                        services.Configure(configureOptions);
                    }
                    services.AddSingleton<ILiteDBBuider, LiteDBBuider>();
                    services.AddSingleton<IMembershipTable, LiteDBClusteringTable>();
                    services.AddSingleton<IConfigurationValidator, LiteDBClusteringSiloOptionsValidator>();
                });
        }

        /// <summary>
        /// Configures this silo to use ADO.NET for clustering. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloBuilder"/>.
        /// </returns>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static ISiloBuilder UseLiteDBClustering(
            this ISiloBuilder builder,
            Action<OptionsBuilder<LiteDBClusteringSiloOptions>> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    configureOptions?.Invoke(services.AddOptions<LiteDBClusteringSiloOptions>());
                    services.AddSingleton<ILiteDBBuider, LiteDBBuider>();
                    services.AddSingleton<IMembershipTable, LiteDBClusteringTable>();
                    services.AddSingleton<IConfigurationValidator, LiteDBClusteringSiloOptionsValidator>();
                });
        }

        /// <summary>
        /// Configures this client to use ADO.NET for clustering. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="IClientBuilder"/>.
        /// </returns>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static IClientBuilder UseLiteDBClustering(
            this IClientBuilder builder,
            Action<LiteDBClusteringClientOptions> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    if (configureOptions != null)
                    {
                        services.Configure(configureOptions);
                    }
                    services.AddSingleton<ILiteDBBuider, LiteDBBuider>();
                    services.AddSingleton<IGatewayListProvider, LiteDBGatewayListProvider>();
                    services.AddSingleton<IConfigurationValidator, LiteDBClusteringClientOptionsValidator>();
                });
        }

        /// <summary>
        /// Configures this client to use ADO.NET for clustering. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="IClientBuilder"/>.
        /// </returns>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static IClientBuilder UseLiteDBClustering(
            this IClientBuilder builder,
            Action<OptionsBuilder<LiteDBClusteringClientOptions>> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    services.AddSingleton<ILiteDBBuider, LiteDBBuider>();
                    configureOptions?.Invoke(services.AddOptions<LiteDBClusteringClientOptions>());
                    services.AddSingleton<IGatewayListProvider, LiteDBGatewayListProvider>();
                    services.AddSingleton<IConfigurationValidator, LiteDBClusteringClientOptionsValidator>();
                });
        }
    }
}
