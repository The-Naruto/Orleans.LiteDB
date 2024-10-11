using Microsoft.Extensions.Options;
using Orleans.Clustering.LiteDB.Messaging;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Clustering.LiteDB.Options
{
    public class LiteDBClusteringClientOptionsValidator
     : IConfigurationValidator
    {
        private readonly LiteDBClusteringClientOptions options;

        public LiteDBClusteringClientOptionsValidator(IOptions<LiteDBClusteringClientOptions> options)
        {
            this.options = options.Value;
        }

        /// <inheritdoc />
        public void ValidateConfiguration()
        {
            if (string.IsNullOrWhiteSpace(this.options.ConnectionString))
            {
                throw new OrleansConfigurationException($"Invalid {nameof(LiteDBClusteringClientOptions)} values for {nameof(LiteDBClusteringTable)}. {nameof(options.ConnectionString)} is required.");
            }
        }
    }
}