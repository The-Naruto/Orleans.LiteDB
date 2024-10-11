using LiteDB.Async;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Clustering.LiteDB
{
    public interface ILiteDBBuider
    { 
        ILiteDatabaseAsync BuildLiteDB(string connectString);

    }

    public class LiteDBBuider : ILiteDBBuider
    {
        public ILiteDatabaseAsync BuildLiteDB(string connectString) => new LiteDatabaseAsync(connectString);
    }
}
