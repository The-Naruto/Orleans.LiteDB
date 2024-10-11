using Orleans.Runtime;
using Orleans.Serialization.WireProtocol;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;

namespace Orleans.Clustering.LiteDB.Entities
{
    public class MemberShip
    {
        public string DepolymentId { get; set; }
        public string Address { get; set; }
        public int Port { get; set; }

        public int Generation { get; set; }
        public string SiloName { get; set; }
        public string HostName { get; set; }
        public int Status { get; set; }
        public int ProxyPort { get; set; }

        public string SuspectTimes { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime IAmAliveTime { get; set; }

    }


    public static class MemberShipExtension
        {

        public static MemberShip ToMemberShip(this MembershipEntry membershipEntry)
        {
          var res =   new MemberShip()
            {
                IAmAliveTime = membershipEntry.IAmAliveTime,
                SiloName = membershipEntry.SiloName,
                HostName = membershipEntry.HostName,
                Generation = membershipEntry.SiloAddress.Generation,
                Port = membershipEntry.SiloAddress.Endpoint.Port,
                Address = membershipEntry.SiloAddress.Endpoint.Address.ToString(),
                StartTime = membershipEntry.StartTime,
                Status = (int)membershipEntry.Status,
                ProxyPort = membershipEntry.ProxyPort,
                
            };

            if (membershipEntry.SuspectTimes.Count>0)
            {
                var timeList = new StringBuilder();
                bool first = true;
                foreach (var tuple in membershipEntry.SuspectTimes)
                {

                    if (!first)
                    {
                        timeList.Append('|');
                    }
                    timeList.Append(tuple.Item1.ToParsableString());
                    timeList.Append(",");
                    timeList.Append(LogFormatter.PrintDate(tuple.Item2));
                    first = false;
                }

                res.SuspectTimes = timeList.ToString();
            }
            else
            {
                res.SuspectTimes = string.Empty;
            }


            return res;
        }

        public static MembershipEntry ToMemberShipEntry(this MemberShip ms)
        {

            var siloAddress = SiloAddress.New(IPAddress.Parse(ms.Address), ms.Port, ms.Generation);

            var entry =    new MembershipEntry
            {
                SiloAddress = siloAddress,
                SiloName = ms.SiloName,
                HostName = ms.HostName,
                Status = (SiloStatus)ms.Status,
                ProxyPort = ms.ProxyPort,
                StartTime = ms.StartTime,
                IAmAliveTime = ms.IAmAliveTime
            };

            if (!string.IsNullOrWhiteSpace(ms.SuspectTimes))
            {
                entry.SuspectTimes = new List<Tuple<SiloAddress, DateTime>>();
                entry.SuspectTimes.AddRange(ms.SuspectTimes.Split('|').Select(s =>
                {
                    var split = s.Split(',');
                    return new Tuple<SiloAddress, DateTime>(SiloAddress.FromParsableString(split[0]),
                        LogFormatter.ParseDate(split[1]));
                }));
            }


            return entry;

        }

    }

}
