using System;

namespace Mars
{
    class Program
    {
        static void Main(string[] args)
        {
            if(args[0]== "start")
            {
                if (args.Length > 1)
                {
                    Cdy.Tag.Runner.RunInstance.StartAsync(args[1]);
                }
                else
                {
                    Cdy.Tag.Runner.RunInstance.Start();
                }
            }

            while (true)
            {
                string cmd = Console.ReadLine();
                if(cmd == "exit")
                {
                    if(Cdy.Tag.Runner.RunInstance.IsStarted)
                    {
                        Cdy.Tag.Runner.RunInstance.Stop();
                    }
                    break;
                }
                else if(cmd=="start")
                {
                    if (args.Length > 0)
                    {
                        Cdy.Tag.Runner.RunInstance.StartAsync(args[0]);
                    }
                    else
                    {
                        Cdy.Tag.Runner.RunInstance.Start();
                    }
                }
                else if(cmd == "stop")
                {
                    Cdy.Tag.Runner.RunInstance.Stop();
                }
            }
        }
    }
}
