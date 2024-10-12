
namespace TC.Grains
{
    public class HelloWorld : Grain, IHelloWorld
    {
        private int count=3;
             

        public Task SayHello()
        {
           string who =  this.GetPrimaryKeyString();

          
                Console.WriteLine($"{who} say hello to user{count}");
           
            return Task.CompletedTask;
        }

        public Task SetTimes(int count)
        {
            this.count = count;

            return Task.CompletedTask;
        }
    }
}
