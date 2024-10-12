using LiteDB;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.LiteDB.Entities;
using Orleans.Clustering.LiteDB.Options;
using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Runtime;
using System.Net;

namespace Orleans.Clustering.LiteDB.Messaging
{
    internal class LiteDBGatewayListProvider : IGatewayListProvider
    {
        private readonly ILogger _logger;
        private readonly string _clusterId;
        private readonly LiteDBClusteringClientOptions _options;
        private readonly IServiceProvider _serviceProvider;
        private readonly TimeSpan _maxStaleness;
        private readonly ILiteDBBuider liteDBBuilder;

        private ILiteDatabase liteDatabaseAsync;

        public LiteDBGatewayListProvider(
          ILogger<LiteDBGatewayListProvider> logger,
          IServiceProvider serviceProvider,
             ILiteDBBuider liteDBBuilder,
          IOptions<LiteDBClusteringClientOptions> options,
          IOptions<GatewayOptions> gatewayOptions,
          IOptions<ClusterOptions> clusterOptions)
        {
            this._logger = logger;
            this._serviceProvider = serviceProvider;
            this._options = options.Value;
            this._clusterId = clusterOptions.Value.ClusterId;
            this._maxStaleness = gatewayOptions.Value.GatewayListRefreshPeriod;
            this.liteDBBuilder = liteDBBuilder;
        }



        public TimeSpan MaxStaleness => this._maxStaleness;

        public bool IsUpdatable => true;

        public Task<IList<Uri>> GetGateways()
        {
            if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("LiteDBClusteringTable.GetGateways called.");
            IList<Uri> uri = new List<Uri>();
            try
            {
                var all = liteDatabaseAsync.GetCollection<MemberShip>();
                var res = all.Find(ms => ms.DepolymentId == this._clusterId && ms.Status == (int)SiloStatus.Active && ms.Port > 0);
                foreach (var item in res.ToArray())
                {
                    var siloAddr = SiloAddress.New(IPAddress.Parse(item.Address), item.ProxyPort, item.Generation);
                    uri.Add(siloAddr.ToGatewayUri());
                }

                return Task.FromResult(uri);
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug(ex, "LiteDBClusteringTable.Gateways failed");
                throw;
            }
        }

        public Task InitializeGatewayListProvider()
        {
            if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("LiteDBClusteringTable.InitializeGatewayListProvider called.");

            this.liteDatabaseAsync = liteDBBuilder.BuildLiteDB(_options.ConnectionString);
            return Task.CompletedTask;
        }
    }
}
