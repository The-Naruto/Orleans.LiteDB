using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TC.Grains
{
    public interface IHelloWorld:IGrainWithStringKey
    {
        Task SetTimes(int count);

        Task SayHello();


    }
}
