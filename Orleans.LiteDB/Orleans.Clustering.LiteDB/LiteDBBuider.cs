using LiteDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Clustering.LiteDB
{
    public interface ILiteDBBuider
    {
        ILiteDatabase BuildLiteDB(string connectString);

    }

    public class LiteDBBuider : ILiteDBBuider
    {
        public ILiteDatabase BuildLiteDB(string connectString) => new LiteDatabase(connectString);
    }
}
