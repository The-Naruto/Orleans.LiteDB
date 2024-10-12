using LiteDB;
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

        private readonly ILiteDatabase liteDatabase;

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
            this.liteDatabase = liteDBBuilder.BuildLiteDB(clusteringTableOptions.ConnectionString);
        }
        /// <summary>
        /// 清理所有当前集群id指定时间之前的所有
        /// </summary>
        /// <param name="beforeDate"></param>
        /// <returns></returns>
        public Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace("IMembershipTable.CleanupDefunctSiloEntries called with beforeDate {beforeDate} and clusterId {ClusterId}.", beforeDate, clusterId);
            try
            {
                ILiteCollection<MemberShip>
                liteCollection = liteDatabase.GetCollection<MemberShip>();

                liteCollection.DeleteMany(ms => ms.DepolymentId == clusterId && ms.IAmAliveTime < beforeDate.LocalDateTime);

            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(ex, "LiteDBClusteringTable.CleanupDefunctSiloEntries failed");
            }

            return Task.CompletedTask;
        }

        public Task DeleteMembershipTableEntries(string clusterId)
        {

            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace("IMembershipTable.DeleteMembershipTableEntries called with clusterId {ClusterId}.", clusterId);
            try
            {
                ILiteCollection<MemberShip>
              liteCollection = liteDatabase.GetCollection<MemberShip>();

                liteCollection.DeleteMany(ms => ms.DepolymentId == clusterId);

                ILiteCollection<MemberShipVersion>
            shipVersion = liteDatabase.GetCollection<MemberShipVersion>();

                shipVersion.DeleteMany(ms => ms.DepolymentId == clusterId);

            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(ex, "LiteDBClusteringTable.DeleteMembershipTableEntries failed");
                throw;
            }
            return Task.CompletedTask;

        }

        public Task InitializeMembershipTable(bool tryInitTableVersion)
        {
            if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("LiteDBClusteringTable.InitializeMembershipTable called.");

            // even if I am not the one who created the table, 
            // try to insert an initial table version if it is not already there,
            // so we always have a first table version row, before this silo starts working.
            if (tryInitTableVersion)
            {
                var wasCreated = InitTable();
                if (wasCreated)
                {
                    logger.LogInformation("Created new table version row.");
                }
            }

            return Task.CompletedTask;
        }

        public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
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

            bool result = false;

            try
            {
                var memberShip = entry.ToMemberShip();
                memberShip.DepolymentId = clusterId;

                ILiteCollection<MemberShip>
             liteCollection = liteDatabase.GetCollection<MemberShip>();
                result = liteCollection.Exists(ms => ms.DepolymentId == clusterId && ms.Address == memberShip.Address && ms.Port == memberShip.Port && ms.Generation == memberShip.Generation);

                if (!result)
                {
                    liteCollection.Insert(memberShip);

                    result = IncreamentVersion();

                }

            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug(ex, "LiteDBClusteringTable.InsertRow failed");
                throw;
            }

            return Task.FromResult(result);
        }

        public Task<MembershipTableData> ReadAll()
        {
            if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("LiteDBClusteringTable.ReadAll called.");
            MembershipTableData membershipTableData;
            try
            {

                ILiteCollection<MemberShip>
             liteCollection = liteDatabase.GetCollection<MemberShip>();

                var allMS = liteCollection.FindAll();

                var tempData = new List<Tuple<MembershipEntry, int>>();


                ILiteCollection<MemberShipVersion>
     shipVersion = liteDatabase.GetCollection<MemberShipVersion>();
                
                foreach (var membership in allMS.ToArray())
                {

                    var tempVersion = shipVersion.FindOne(sv => sv.DepolymentId == membership.DepolymentId);

                    tempData.Add(Tuple.Create(membership.ToMemberShipEntry(), tempVersion.Version));
                }

                if (tempData.Count > 0)
                {
                    membershipTableData = ConvertToMembershipTableData(tempData);
                }
                else
                {

                    membershipTableData = new MembershipTableData(new TableVersion(0, "0"));
                }
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug(ex, "LiteDBClusteringTable.ReadAll failed");
                throw;
            }
            return Task.FromResult(membershipTableData);
        }

        public Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace("LiteDBClusteringTable.ReadRow called with key: {Key}.", key);
            MembershipTableData membershipTableData;
            try
            {
                ILiteCollection<MemberShip>
           liteCollection = liteDatabase.GetCollection<MemberShip>();

                var allMS = liteCollection.Find(ms => ms.DepolymentId == clusterId && ms.Address == key.Endpoint.Address.ToString() && ms.Port == key.Endpoint.Port && ms.Generation == key.Generation);

                var tempData = new List<Tuple<MembershipEntry, int>>();


                ILiteCollection<MemberShipVersion>
     shipVersion = liteDatabase.GetCollection<MemberShipVersion>();
                MemberShipVersion? tempVersion = null;
                foreach (var membership in allMS.ToArray())
                {
                    if (tempVersion == null)
                    {

                        tempVersion = shipVersion.FindOne(sv => sv.DepolymentId == membership.DepolymentId);
                    }


                    tempData.Add(Tuple.Create(membership.ToMemberShipEntry(), tempVersion.Version));
                }
                if (tempData.Count > 0)
                {
                    membershipTableData = ConvertToMembershipTableData(tempData);
                }
                else
                {

                    membershipTableData = new MembershipTableData(new TableVersion(0, "0"));
                }
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug(ex, "LiteDBClusteringTable.ReadRow failed");
                throw;
            }
            return Task.FromResult(membershipTableData);
        }

        public Task UpdateIAmAlive(MembershipEntry entry)
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

                ILiteCollection<MemberShip>
    liteCollection = liteDatabase.GetCollection<MemberShip>();

                var old = liteCollection.FindOne(ms => ms.DepolymentId == clusterId && ms.Address == newShip.Address && ms.Port == newShip.Port && ms.Generation == newShip.Generation);
                old.IAmAliveTime = newShip.IAmAliveTime;

                liteCollection.Update(old);
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(ex, "LiteDBClusteringTable.UpdateIAmAlive failed");
                throw;
            }
            return Task.CompletedTask;
        }

        public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
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
            bool versinoResult = false;
            try
            {
                versinoResult = IncreamentVersion();
                if (!versinoResult)
                {
                    return Task.FromResult(versinoResult);
                }
                var newShip = entry.ToMemberShip();

                ILiteCollection<MemberShip> liteCollection = liteDatabase.GetCollection<MemberShip>();

                var old = liteCollection.FindOne(ms => ms.DepolymentId == clusterId && ms.Address == newShip.Address && ms.Port == newShip.Port && ms.Generation == newShip.Generation);
                old.Status = newShip.Status;
                old.SuspectTimes = newShip.SuspectTimes;
                old.IAmAliveTime = newShip.IAmAliveTime;
                versinoResult = liteCollection.Update(old);

            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.LogDebug(ex, "LiteDBClusteringTable.UpdateRow failed");
                throw;
            }
            return Task.FromResult(versinoResult);
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


        private bool IncreamentVersion()
        {

            ILiteCollection<MemberShipVersion>
     shipVersion = liteDatabase.GetCollection<MemberShipVersion>();

            var res = shipVersion.FindOne(ms => ms.DepolymentId == clusterId);
            res.Version++;

            return shipVersion.Update(res);
        }



        private bool InitTable()
        {
            try
            {
                ILiteCollection<MemberShipVersion>
       shipVersion = liteDatabase.GetCollection<MemberShipVersion>();
                var res = shipVersion.Exists(ms => ms.DepolymentId == clusterId);
                if (res)
                {
                    return true;
                }

                shipVersion.Insert(new MemberShipVersion() { DepolymentId = clusterId, TimeStamp = DateTime.Now, Version = 0 });

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
