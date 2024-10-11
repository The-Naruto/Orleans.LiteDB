using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Clustering.LiteDB.Options
{
    public class LiteDBClusteringClientOptions
    {
        /// <summary>
        /// Connection string for Sql
        /// </summary>
        [Redact]
        public string ConnectionString { get; set; }

    }
}
