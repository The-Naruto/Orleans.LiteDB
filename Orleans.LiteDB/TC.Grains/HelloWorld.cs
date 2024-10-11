
namespace TC.Grains
{
    public class HelloWorld : Grain, IHelloWorld
    {
        private int count=3;
             

        public Task SayHello()
        {
           string who =  this.GetPrimaryKeyString();

            for (int i = 0; i < count; i++)
            {

                Console.WriteLine($"{who} say hello {i}");
            }

            return Task.CompletedTask;
        }

        public Task SetTimes(int count)
        {
            this.count = count;

            return Task.CompletedTask;
        }
    }
}
