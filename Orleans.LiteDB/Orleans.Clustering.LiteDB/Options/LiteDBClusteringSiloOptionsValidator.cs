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
    public class LiteDBClusteringSiloOptionsValidator
     : IConfigurationValidator
    {
        private readonly LiteDBClusteringSiloOptions options;

        public LiteDBClusteringSiloOptionsValidator(IOptions<LiteDBClusteringSiloOptions> options)
        {
            this.options = options.Value;
        }

        /// <inheritdoc />
        public void ValidateConfiguration()
        {
            if (string.IsNullOrWhiteSpace(this.options.ConnectionString))
            {
                throw new OrleansConfigurationException($"Invalid {nameof(LiteDBClusteringSiloOptions)} values for {nameof(LiteDBClusteringTable)}. {nameof(options.ConnectionString)} is required.");
            }
        }
    }
}