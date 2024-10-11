using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Clustering.LiteDB.Entities
{
    public class MemberShipVersion
    {
        public string  DepolymentId {  get; set; }

        public DateTime TimeStamp { get; set; }

        public int Version { get; set; }
    }
}
