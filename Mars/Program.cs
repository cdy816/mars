using System;

namespace Mars
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length > 0)
            {
                Cdy.Tag.Runner.RunInstance.StartAsync(args[0]);
            }
            else
            {
                Cdy.Tag.Runner.RunInstance.Start();
            }
            while (true)
            {
                var skey = Console.ReadKey();
                if(skey.Key == ConsoleKey.Escape)
                {
                    Cdy.Tag.Runner.RunInstance.Stop();
                }
            }
        }
    }
}
