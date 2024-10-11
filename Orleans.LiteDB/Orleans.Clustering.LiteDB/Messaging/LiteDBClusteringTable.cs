using LiteDB.Async;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.LiteDB.Entities;
using Orleans.Clustering.LiteDB.Options;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Orleans.Clustering.LiteDB.Messaging
{
    public class LiteDBClusteringTable : IMembershipTable
    {
        private readonly string clusterId;
        private readonly IServiceProvider serviceProvider;
        private readonly ILogger logger;
        private readonly LiteDBClusteringSiloOptions clusteringTableOptions;

        private readonly ILiteDatabaseAsync liteDatabaseAsync;

        public LiteDBClusteringTable(
            IServiceProvider serviceProvider,
            IOptions<ClusterOptions> clusterOptions,
            IOptions<LiteDBClusteringSiloOptions> clusteringOptions,
            ILiteDBBuider liteDBBuilder,
            ILogger<LiteDBClusteringTable> logger)
        {
            this.serviceProvider = serviceProvider;
            this.logger = logger;
            this.clusteringTableOptions = clusteringOptions.Value;
            this.clusterId = clusterOptions.Value.ClusterId;
            this.liteDatabaseAsync = liteDBBuilder.BuildLiteDB(clusteringTableOptions.ConnectionString);
        }
        /// <summary>
        /// 清理所有当前集群id指定时间之前的所有
        /// </summary>
        /// <param name="beforeDate"></param>
        /// <returns></returns>
        public async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace("IMembershipTable.CleanupDefunctSiloEntries called with beforeDate {beforeDate} and clusterId {ClusterId}.", beforeDate, clusterId);
            try
            {
                ILiteCollectionAsync<MemberShip>
                liteCollectionAsync = liteDatabaseAsync.GetCollection<MemberShip>();

                await liteCollectionAsync.DeleteManyAsync(ms => ms.DepolymentId == clusterId && ms.IAmAliveTime < beforeDate.LocalDateTime);
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(ex, "LiteDBClusteringTable.CleanupDefunctSiloEntries failed");
            }
        }

        public async Task DeleteMembershipTableEntries(string clusterId)
        {

            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace("IMembershipTable.DeleteMembershipTableEntries called with clusterId {ClusterId}.", clusterId);
            try
            {
                ILiteCollectionAsync<MemberShip>
              liteCollectionAsync = liteDatabaseAsync.GetCollection<MemberShip>();

                await liteCollectionAsync.DeleteManyAsync(ms => ms.DepolymentId == clusterId);

                ILiteCollectionAsync<MemberShipVersion>
            shipVersion = liteDatabaseAsync.GetCollection<MemberShipVersion>();

                await shipVersion.DeleteManyAsync(ms => ms.DepolymentId == clusterId);

            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(ex, "LiteDBClusteringTable.DeleteMembershipTableEntries failed");
                throw;
            }


        }

        public async Task InitializeMembershipTable(bool tryInitTableVersion)
        {
            if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("LiteDBClusteringTable.InitializeMembershipTable called.");

            // even if I am not the one who created the table, 
            // try to insert an initial table version if it is not already there,
            // so we always have a first table version row, before this silo starts working.
            if (tryInitTableVersion)
            {
                var wasCreated = await InitTableAsync();
                if (wasCreated)
                {
                    logger.LogInformation("Created new table version row.");
                }
            }


        }

        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace(
                    "LiteDBClusteringTable.InsertRow called with entry {Entry} and tableVersion {TableVersion}.",
                    entry,
                    tableVersion);


            if (entry == null)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug("LiteDBClusteringTable.InsertRow aborted due to null check. MembershipEntry is null.");
                throw new ArgumentNullException(nameof(entry));
            }
            if (tableVersion is null)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug("LiteDBClusteringTable.InsertRow aborted due to null check. TableVersion is null ");
                throw new ArgumentNullException(nameof(tableVersion));
            }

            try
            {
                var memberShip = entry.ToMemberShip();
                memberShip.DepolymentId = clusterId;

                ILiteCollectionAsync<MemberShip>
             liteCollectionAsync = liteDatabaseAsync.GetCollection<MemberShip>();
                var exists = await liteCollectionAsync.ExistsAsync(ms => ms.DepolymentId == clusterId && ms.Address == memberShip.Address && ms.Port == memberShip.Port && ms.Generation == memberShip.Generation);

                if (exists) { return true; }

                await liteCollectionAsync.InsertAsync(memberShip);

                return await IncreamentVersion();

            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug(ex, "LiteDBClusteringTable.InsertRow failed");
                throw;
            }

        }

        public async Task<MembershipTableData> ReadAll()
        {
            if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("LiteDBClusteringTable.ReadAll called.");
            try
            {

                ILiteCollectionAsync<MemberShip>
             liteCollectionAsync = liteDatabaseAsync.GetCollection<MemberShip>();

                var allMS = await liteCollectionAsync.FindAllAsync();

                var tempData = new List<Tuple<MembershipEntry, int>>(allMS.Count());


                ILiteCollectionAsync<MemberShipVersion>
     shipVersion = liteDatabaseAsync.GetCollection<MemberShipVersion>();

                foreach (var membership in allMS)
                {

                    var tempVersion = await shipVersion.FindOneAsync(sv => sv.DepolymentId == membership.DepolymentId);

                    tempData.Add(Tuple.Create(membership.ToMemberShipEntry(), tempVersion.Version));
                }

                MembershipTableData membershipTableData;
                if (tempData.Count > 0)
                {
                    membershipTableData = ConvertToMembershipTableData(tempData);
                }
                else
                {

                    membershipTableData = new MembershipTableData(new TableVersion(0, "0"));
                }
                return membershipTableData;
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug(ex, "LiteDBClusteringTable.ReadAll failed");
                throw;
            }
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace("LiteDBClusteringTable.ReadRow called with key: {Key}.", key);
            try
            {
                ILiteCollectionAsync<MemberShip>
           liteCollectionAsync = liteDatabaseAsync.GetCollection<MemberShip>();

                var allMS = await liteCollectionAsync.FindAsync(ms => ms.DepolymentId == clusterId && ms.Address == key.Endpoint.Address.ToString() && ms.Port == key.Endpoint.Port && ms.Generation == key.Generation);

                var tempData = new List<Tuple<MembershipEntry, int>>(allMS.Count());


                ILiteCollectionAsync<MemberShipVersion>
     shipVersion = liteDatabaseAsync.GetCollection<MemberShipVersion>();
                MemberShipVersion? tempVersion = null;
                foreach (var membership in allMS)
                {
                    if (tempVersion == null)
                    {

                        tempVersion = await shipVersion.FindOneAsync(sv => sv.DepolymentId == membership.DepolymentId);
                    }


                    tempData.Add(Tuple.Create(membership.ToMemberShipEntry(), tempVersion.Version));
                }
                MembershipTableData membershipTableData;
                if (tempData.Count > 0)
                {
                    membershipTableData = ConvertToMembershipTableData(tempData);
                }
                else
                {

                    membershipTableData = new MembershipTableData(new TableVersion(0, "0"));
                }
                return membershipTableData;
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug(ex, "LiteDBClusteringTable.ReadRow failed");
                throw;
            }
        }

        public async Task UpdateIAmAlive(MembershipEntry entry)
        {
            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace("IMembershipTable.UpdateIAmAlive called with entry {Entry}.", entry);
            if (entry == null)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug("LiteDBClusteringTable.UpdateIAmAlive aborted due to null check. MembershipEntry is null.");
                throw new ArgumentNullException(nameof(entry));
            }
            try
            {
                var newShip = entry.ToMemberShip();

                ILiteCollectionAsync<MemberShip>
    liteCollectionAsync = liteDatabaseAsync.GetCollection<MemberShip>();

                var old = await liteCollectionAsync.FindOneAsync(ms => ms.DepolymentId == clusterId && ms.Address == newShip.Address && ms.Port == newShip.Port && ms.Generation == newShip.Generation);
                old.IAmAliveTime = newShip.IAmAliveTime;

                await liteCollectionAsync.UpdateAsync(old);
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(ex, "LiteDBClusteringTable.UpdateIAmAlive failed");
                throw;
            }
        }

        public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("IMembershipTable.UpdateRow called with entry {Entry}, etag {ETag} and tableVersion {TableVersion}.", entry, etag, tableVersion);

            //The "tableVersion" parameter should always exist when updating a row as Init should
            //have been called and membership version created and read. This is an optimization to
            //not to go through all the way to database to fail a conditional check (which does
            //exist for the sake of robustness) as mandated by Orleans membership protocol.
            //Likewise, no update can be done without membership entry or an etag.
            if (entry == null)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug("LiteDBClusteringTable.UpdateRow aborted due to null check. MembershipEntry is null.");
                throw new ArgumentNullException(nameof(entry));
            }
            if (tableVersion is null)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug("LiteDBClusteringTable.UpdateRow aborted due to null check. TableVersion is null");
                throw new ArgumentNullException(nameof(tableVersion));
            }

            try
            {
                var versinoResult = await IncreamentVersion();
                if (!versinoResult)
                {
                    return versinoResult;
                }
                var newShip = entry.ToMemberShip();

                ILiteCollectionAsync<MemberShip>
    liteCollectionAsync = liteDatabaseAsync.GetCollection<MemberShip>();

                var old = await liteCollectionAsync.FindOneAsync(ms => ms.DepolymentId == clusterId && ms.Address == newShip.Address && ms.Port == newShip.Port && ms.Generation == newShip.Generation);
                old.Status = newShip.Status;
                old.SuspectTimes = newShip.SuspectTimes;
                old.IAmAliveTime = newShip.IAmAliveTime;
                return await liteCollectionAsync.UpdateAsync(old);

            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug(ex, "LiteDBClusteringTable.UpdateRow failed");
                throw;
            }

        }

        private MembershipTableData ConvertToMembershipTableData(IEnumerable<Tuple<MembershipEntry, int>> ret)
        {
            var retList = ret.ToList();
            var tableVersionEtag = retList[0].Item2;
            var membershipEntries = new List<Tuple<MembershipEntry, string>>();
            if (retList[0].Item1 != null)
            {
                membershipEntries.AddRange(retList.Select(i => new Tuple<MembershipEntry, string>(i.Item1, string.Empty)));
            }
            return new MembershipTableData(membershipEntries, new TableVersion(tableVersionEtag, tableVersionEtag.ToString()));
        }


        private async Task<bool> IncreamentVersion()
        {

            ILiteCollectionAsync<MemberShipVersion>
     shipVersion = liteDatabaseAsync.GetCollection<MemberShipVersion>();

            var res = await shipVersion.FindOneAsync(ms => ms.DepolymentId == clusterId);

            res.Version++;

            return await shipVersion.UpdateAsync(res);
        }



        private async Task<bool> InitTableAsync()
        {
            try
            {
                ILiteCollectionAsync<MemberShipVersion>
       shipVersion = liteDatabaseAsync.GetCollection<MemberShipVersion>();
                var res = await shipVersion.ExistsAsync(ms => ms.DepolymentId == clusterId);
                if (res)
                {
                    return true;
                }

                await shipVersion.InsertAsync(new MemberShipVersion() { DepolymentId = clusterId, TimeStamp = DateTime.Now, Version = 0 });

                return true;
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace(ex, "Insert silo membership version failed");
                throw;
            }
        }
    }
}
